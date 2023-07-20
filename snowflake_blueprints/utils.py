import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization.ssh import serialize_ssh_public_key

def decode_rsa(rsa_key: str, passphrase: str):
    """ Helper function to decode a private key file

    Args:
        rsa_key (str): The file path of the key file to decode

    Returns:
        _type_: 
    """
    try:
        with open(rsa_key, "rb") as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                # password=os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"].encode(),
                password = passphrase.encode(),
                backend=default_backend(),
            )
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        return pkb


    except Exception as e:
        print("Error in reading the private key file, please check the file path")
        return None


        