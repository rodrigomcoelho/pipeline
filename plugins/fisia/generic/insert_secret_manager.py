# Import the Secret Manager client library.
import json

from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from google.cloud import secretmanager


def create_secret(**kwargs):

    project_id = kwargs["project_id"]
    secret_id = kwargs["secret_id"]

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the parent project.
    parent = f"projects/{project_id}"

    # Create the secret.
    response = client.create_secret(
        request={
            "parent": parent,
            "secret_id": secret_id,
            "secret": {"replication": {"automatic": {}}},
        }
    )


def create_secret_version(**kwargs):

    project_id = kwargs["project_id"]
    secret_id = kwargs["secret_id"]
    data = kwargs["data"]

    key = ChaCha20Poly1305.generate_key()

    data = json.load(data)
    data = data.append(key)

    client = secretmanager.SecretManagerServiceClient()
    parent = client.secret_path(project_id, secret_id)
    response = client.add_secret_version(
        request={"parent": parent, "payload": {"data": data.encode("UTF-8")}}
    )
    return response
