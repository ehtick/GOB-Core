import jwt

REQUEST_ACCESS_TOKEN = 'X-Forwarded-Access-Token'


def extract_roles(headers: dict):
    """Extracts roles from given request headers

    :param headers:
    :return:
    """
    access_token = headers.get(REQUEST_ACCESS_TOKEN)
    if access_token:
        decoded = jwt.decode(access_token, verify=False)
        return decoded.get('realm_access', {}).get('roles', [])
    return []


def is_secured_request(headers: dict):
    return True if headers.get(REQUEST_ACCESS_TOKEN) else False
