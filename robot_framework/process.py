"""This module contains the main process of the robot."""

import json
import os
import re
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueStatus
from itk_dev_shared_components.graph import authentication as graph_authentication
from itk_dev_shared_components.graph.authentication import GraphAccess
from itk_dev_shared_components.graph import mail as graph_mail
from itk_dev_shared_components.graph.mail import Email
from itk_dev_shared_components.kmd_nova.authentication import NovaAccess
from itk_dev_shared_components.kmd_nova.nova_objects import NovaCase, Document, CaseParty, Department, Caseworker
from itk_dev_shared_components.kmd_nova import nova_cases, nova_documents
from itk_dev_shared_components.kmd_nova import cpr as nova_cpr

from robot_framework import config


def process(orchestrator_connection: OrchestratorConnection) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")

    graph_creds = orchestrator_connection.get_credential(config.GRAPH_API)
    graph_access = graph_authentication.authorize_by_username_password(graph_creds.username, **json.loads(graph_creds.password))

    nova_creds = orchestrator_connection.get_credential(config.NOVA_API)
    nova_access = NovaAccess(nova_creds.username, nova_creds.password)

    emails = get_emails(graph_access)
    orchestrator_connection.log_info(f"Number of emails to journalize: {len(emails)}")

    # Create a partial function with the static keywords set
    p_handle_email = partial(handle_email, graph_access=graph_access, nova_access=nova_access, orchestrator_connection=orchestrator_connection)

    for email in emails:
        p_handle_email(email)


def handle_email(email: Email, graph_access: GraphAccess, nova_access: NovaAccess, orchestrator_connection: OrchestratorConnection) -> None:
    """Handle the processing of a single email."""
    cpr = get_cpr_from_email(email)
    case_date = datetime.fromisoformat(email.received_time)

    queue_element = orchestrator_connection.create_queue_element(config.QUEUE_NAME, reference=f"{cpr} - {email.received_time}")

    try:
        case = create_case(cpr, case_date, nova_access)
        attach_email_to_case(email, case, graph_access, nova_access)

        graph_mail.move_email(email, "Indbakke/Enlig forsørgerprojekt/Journaliserede kvitteringer", graph_access)
    except Exception as exc:
        orchestrator_connection.set_queue_element_status(queue_element.id, QueueStatus.FAILED, message=str(exc))
        raise exc

    orchestrator_connection.set_queue_element_status(queue_element.id, QueueStatus.DONE)


def get_emails(graph_access: GraphAccess) -> list[Email]:
    """Get all emails to be handled by the robot.

    Args:
        graph_access: The GraphAccess object used to authenticate against Graph.

    Returns:
        A filtered list of email objects to be handled.
    """
    # Get all emails from the 'Refusioner' folder.
    mails = graph_mail.get_emails_from_folder("kontrolteamet@mkb.aarhus.dk", "Indbakke/Enlig forsørgerprojekt/Kvitteringer til journalisering", graph_access)

    # Filter the emails on sender and subject
    mails = [mail for mail in mails if mail.sender == "noreply@aarhus.dk" and mail.subject == 'Erklæring - økonomisk fripladstilskud']

    return mails


def get_cpr_from_email(email: Email) -> str:
    """Get the relevant cpr number from the email body.

    Args:
        email: The email object.

    Returns:
        The cpr number.
    """
    text = email.get_text()
    cpr = re.findall(r"CPR-nummer(.+?)Adresse", text)[0]
    return cpr


def create_case(cpr: str, case_date: datetime, nova_access: NovaAccess) -> NovaCase:
    """Find a case with the correct title and kle number on the given cpr.
    If no case exists a new one is created instead.

    Args:
        cpr: The cpr of the person to get the case from.
        nova_access: The nova access object used to authenticate.

    Returns:
        The relevant nova case.
    """
    # Find the name of the person by looking up the cpr number
    name = nova_cpr.get_address_by_cpr(cpr, nova_access)['name']

    case_party = CaseParty(
        role="Primær",
        identification_type="CprNummer",
        identification=cpr,
        name=name,
        uuid=None
    )

    case_title = f"Projekt enlig forsørger {datetime.now().year}"

    department = Department(
        id=70363,
        name="Kontrolteamet",
        user_key="4BKONTROL"
    )

    caseworker = Caseworker(
        uuid="3eb31cbb-d6dc-4b01-9eec-0b415f5b89cb",
        name="Rpabruger Rpa15 - MÅ IKKE SLETTES RITM0055928",
        ident="AZRPA15"
    )

    # Create a new case
    case = NovaCase(
        uuid=str(uuid.uuid4()),
        title=case_title,
        case_date=case_date,
        progress_state='Afsluttet',
        case_parties=[case_party],
        kle_number="32.45.04",
        proceeding_facet="G01",
        sensitivity="Fortrolige",
        responsible_department=department,
        security_unit=department,
        caseworker=caseworker
    )
    nova_cases.add_case(case, nova_access)
    return case


def attach_email_to_case(email: Email, case: NovaCase, graph_access: GraphAccess, nova_access: NovaAccess):
    """Upload the email file to Nova as a document.

    Args:
        email: The email object to upload.
        case: The nova case to attach to.
        graph_access: The graph access object used to authenticate against Graph.
        nova_access: The nova access object used to authenticate against Nova.
    """
    mime = graph_mail.get_email_as_mime(email, graph_access)
    doc_uuid = nova_documents.upload_document(mime, "Email kvittering.eml", nova_access)

    doc = Document(
        uuid=doc_uuid,
        title="Email kvittering",
        sensitivity="Fortrolige",
        document_type="Indgående",
        document_date=email.received_time,
        approved=True,
        description="Automatisk journaliseret af robot.",
    )

    nova_documents.attach_document_to_case(case.uuid, doc, nova_access)


if __name__ == '__main__':
    conn_string = os.getenv("OpenOrchestratorConnString")
    crypto_key = os.getenv("OpenOrchestratorKey")
    oc = OrchestratorConnection("Enlig forsørger test", conn_string, crypto_key, "ghbm@aarhus.dk")
    process(oc)
