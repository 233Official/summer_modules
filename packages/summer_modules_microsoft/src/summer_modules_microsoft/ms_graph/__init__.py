class MSGraph:
    """
    Microsoft Graph API 通用类
    """

    def __init__(self, access_token: str):
        """
        初始化 MSGraph 类

        :param access_token: Microsoft Graph API 的访问令牌
        """
        self.access_token = access_token
        self.base_url = "https://graph.microsoft.com/v1.0/me"
