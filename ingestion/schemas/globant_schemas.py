from pydantic import BaseModel, field_validator
from datetime import datetime
from typing import Dict, Type

class DepartmentSchema(BaseModel):
    id: int
    department: str

    @field_validator('department')
    @classmethod
    def department_not_empty(cls, v):
        if not v or not isinstance(v, str) or not v.strip():
            raise ValueError('department cannot be empty')
        return v.strip()

class JobSchema(BaseModel):
    id: int
    job: str

    @field_validator('job')
    @classmethod
    def job_not_empty(cls, v):
        if not v or not isinstance(v, str) or not v.strip():
            raise ValueError('job cannot be empty')
        return v.strip()

class HiredEmployeeSchema(BaseModel):
    id: int
    name: str
    datetime: str
    department_id: int
    job_id: int

    @field_validator('datetime')
    @classmethod
    def validate_iso_datetime(cls, v):
        if not v or not isinstance(v, str):
            raise ValueError('datetime must be a non-empty string')
        try:
            # ISO 8601 format check. Replace Z with utc offset to support standard ISO strings
            datetime.fromisoformat(v.replace('Z', '+00:00'))
        except ValueError:
            raise ValueError(f'datetime must be ISO 8601 format, got: {v}')
        return v

    @field_validator('name')
    @classmethod
    def name_not_empty(cls, v):
        if not v or not isinstance(v, str) or not v.strip():
            raise ValueError('name cannot be empty')
        return v.strip()

TABLE_SCHEMAS: Dict[str, Type[BaseModel]] = {
    'departments': DepartmentSchema,
    'jobs': JobSchema,
    'hired_employees': HiredEmployeeSchema,
}
