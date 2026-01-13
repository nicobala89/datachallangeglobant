"""
Base Connector Abstract Class
Defines the interface for all connectors in the Gorigami Data Framework.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from loguru import logger
import yaml
import os
from pathlib import Path


class ConnectorError(Exception):
    """Base exception for connector errors"""
    pass


class ConnectionValidationError(ConnectorError):
    """Raised when connection validation fails"""
    pass


class ConfigurationError(ConnectorError):
    """Raised when connector configuration is invalid"""
    pass


class BaseConnector(ABC):
    """
    Abstract base class for all connectors.
    
    Connectors are responsible for:
    - Validating connections to data sources
    - Verifying entity/document existence
    - Returning connection metadata
    - Lightweight operations (no data transfer)
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the connector with configuration.

        Args:
            config: Configuration dictionary for the connector
        """
        self.config = config
        self._validate_config()
        self._connected = False
        logger.info(f"Initialized {self.__class__.__name__} connector")

    @abstractmethod
    def _validate_config(self) -> None:
        """
        Validate the connector configuration.
        
        Raises:
            ConfigurationError: If configuration is invalid
        """
        pass

    @abstractmethod
    def validate(self) -> bool:
        """
        Validate the connection and verify entity exists.
        
        This should be a lightweight operation that:
        - Tests connectivity
        - Verifies credentials/permissions
        - Checks if the target entity exists
        
        Returns:
            True if validation succeeds
            
        Raises:
            ConnectionValidationError: If validation fails
        """
        pass

    @abstractmethod
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the connection/entity without transferring data.
        
        Returns:
            Dictionary containing metadata such as:
            - Entity name/path
            - Size/row count
            - Last modified timestamp
            - Format/type
            - Version information
        """
        pass

    def close(self) -> None:
        """
        Close the connection and cleanup resources.
        
        Default implementation does nothing. Override if needed.
        """
        if self._connected:
            logger.info(f"Closing {self.__class__.__name__} connector")
            self._connected = False

    @classmethod
    def from_yaml(cls, config_path: str) -> 'BaseConnector':
        """
        Create a connector instance from a YAML configuration file.

        Args:
            config_path: Path to YAML configuration file

        Returns:
            Connector instance

        Raises:
            ConfigurationError: If configuration file is invalid
        """
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            # Substitute environment variables
            config = cls._substitute_env_vars(config)
            
            return cls(config)
        except FileNotFoundError:
            raise ConfigurationError(f"Configuration file not found: {config_path}")
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML configuration: {e}")

    @staticmethod
    def _substitute_env_vars(config: Any) -> Any:
        """
        Recursively substitute environment variables in configuration.
        
        Supports ${VAR_NAME} syntax.

        Args:
            config: Configuration value (dict, list, str, etc.)

        Returns:
            Configuration with environment variables substituted
        """
        if isinstance(config, dict):
            return {k: BaseConnector._substitute_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [BaseConnector._substitute_env_vars(item) for item in config]
        elif isinstance(config, str):
            # Replace ${VAR_NAME} with environment variable value
            if config.startswith('${') and config.endswith('}'):
                var_name = config[2:-1]
                value = os.getenv(var_name)
                if value is None:
                    logger.warning(f"Environment variable {var_name} not set")
                    return config
                return value
            return config
        else:
            return config

    def _require_config(self, *keys: str) -> None:
        """
        Validate that required configuration keys are present.

        Args:
            *keys: Required configuration keys

        Raises:
            ConfigurationError: If any required key is missing
        """
        for key in keys:
            if key not in self.config:
                raise ConfigurationError(f"Missing required configuration: {key}")

    def __enter__(self):
        """Context manager entry"""
        self.validate()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

    def __repr__(self) -> str:
        """String representation"""
        return f"{self.__class__.__name__}(connected={self._connected})"
