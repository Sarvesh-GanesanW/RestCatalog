from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    databaseUrl: str
    icebergWarehousePath: str

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
