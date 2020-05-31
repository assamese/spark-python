
class ConfigFramework:
    AWS_SECRET = 'xxx'  ## PLUG IN YOUR AWS SECRET
    AWS_KEY = 'xxx'

    POSTGRES_ADDRESS = 'raja.db.elephantsql.com'  ## INSERT YOUR DB ADDRESS
    POSTGRES_PORT = '5432'
    POSTGRES_DBNAME = 'ljalsmbf'  ## CHANGE THIS TO YOUR DATABASE NAME

    POSTGRES_PROPERTIES = {
        'user': "ljalsmbf", ## CHANGE THIS TO YOUR USER NAME
        'password': "xxx" ## CHANGE THIS TO YOUR PASSWORD
    }
    @staticmethod
    def getAWS_Secret():
        return ConfigFramework.AWS_SECRET

    @staticmethod
    def getAWS_Key():
        return ConfigFramework.AWS_KEY

    @staticmethod
    def getPostgres_URL():
        return ('jdbc:postgresql://{ipaddress}:{port}/{dbname}'
                .format(ipaddress=ConfigFramework.POSTGRES_ADDRESS
                        , port=ConfigFramework.POSTGRES_PORT
                        , dbname=ConfigFramework.POSTGRES_DBNAME))

    @staticmethod
    def getPostgres_Properties():
        return ConfigFramework.POSTGRES_PROPERTIES
