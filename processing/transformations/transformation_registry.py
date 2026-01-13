"""
Transformation Registry
Registry for reusable transformation functions.
"""

from typing import Callable, Dict, Any, List
from pyspark.sql import DataFrame
from loguru import logger


class TransformationRegistry:
    """
    Registry for transformation functions.
    
    Allows registration, composition, and versioning of transformations.
    """

    def __init__(self):
        self._transformations: Dict[str, Dict[str, Any]] = {}

    def register(
        self,
        name: str,
        func: Callable[[DataFrame], DataFrame],
        version: str = "1.0",
        description: str = ""
    ) -> None:
        """
        Register a transformation function.

        Args:
            name: Unique transformation name
            func: Transformation function (DataFrame -> DataFrame)
            version: Transformation version
            description: Human-readable description
        """
        self._transformations[name] = {
            'func': func,
            'version': version,
            'description': description
        }
        logger.info(f"Registered transformation: {name} (v{version})")

    def get(self, name: str) -> Callable[[DataFrame], DataFrame]:
        """
        Get a registered transformation function.

        Args:
            name: Transformation name

        Returns:
            Transformation function

        Raises:
            KeyError: If transformation not found
        """
        if name not in self._transformations:
            raise KeyError(f"Transformation '{name}' not registered")
        
        return self._transformations[name]['func']

    def compose(self, *transformation_names: str) -> Callable[[DataFrame], DataFrame]:
        """
        Compose multiple transformations into a single function.

        Args:
            *transformation_names: Names of transformations to compose

        Returns:
            Composed transformation function
        """
        def composed_transformation(df: DataFrame) -> DataFrame:
            for name in transformation_names:
                func = self.get(name)
                df = func(df)
                logger.debug(f"Applied transformation: {name}")
            return df
        
        return composed_transformation

    def list_transformations(self) -> List[Dict[str, Any]]:
        """
        List all registered transformations.

        Returns:
            List of transformation metadata
        """
        return [
            {
                'name': name,
                'version': meta['version'],
                'description': meta['description']
            }
            for name, meta in self._transformations.items()
        ]

    def __repr__(self) -> str:
        return f"TransformationRegistry({len(self._transformations)} transformations)"


# Global registry instance
registry = TransformationRegistry()
