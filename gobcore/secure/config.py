"""
User authorization levels and roles

"""

# Keycloak header attributes
AUTH_PATTERN = '^X-Auth-'
REQUEST_USER = 'X-Auth-Userid'
REQUEST_ROLES = 'X-Auth-Roles'

# Keycloak roles
GOB_ADMIN = "gob_adm"
BRK_DATA_TOTAAL = "BRK_Data_Totaal"
BRK_DATA_BEPERKT = "BRK_Data_Beperkt"
