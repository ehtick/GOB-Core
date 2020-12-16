import jwt

ACCESS_TOKEN_HEADER = 'X-Forwarded-Access-Token'
USER_EMAIL_HEADER = 'X-Forwarded-Email'


def extract_roles(headers: dict):
    """Extracts roles from given request headers

    :param headers:
    :return:
    """
    access_token = headers.get(ACCESS_TOKEN_HEADER)
    if access_token:
        decoded = jwt.decode(access_token, verify=False)
        return decoded.get('realm_access', {}).get('roles', [])
    return []


def is_secured_request(headers: dict):
    return True if headers.get(ACCESS_TOKEN_HEADER) else False
