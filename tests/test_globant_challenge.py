import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from pydantic import ValidationError

from ingestion.schemas.globant_schemas import DepartmentSchema, JobSchema, HiredEmployeeSchema
from catalog.api.catalog_api import app, get_db

# ============================================================================
# Pydantic Schema Tests
# ============================================================================

def test_department_validation():
    # Valid department
    dept = DepartmentSchema(id=1, department="Engineering")
    assert dept.id == 1
    assert dept.department == "Engineering"

    # Invalid empty department
    with pytest.raises(ValidationError):
        DepartmentSchema(id=1, department="   ")

def test_job_validation():
    # Valid job
    job = JobSchema(id=12, job="Data Engineer")
    assert job.id == 12
    assert job.job == "Data Engineer"

    # Invalid empty job
    with pytest.raises(ValidationError):
        JobSchema(id=12, job="")

def test_hired_employee_validation():
    # Valid hired employee
    emp = HiredEmployeeSchema(
        id=101,
        name="John Doe",
        datetime="2021-11-07T02:48:00Z",
        department_id=5,
        job_id=96
    )
    assert emp.name == "John Doe"
    assert emp.datetime == "2021-11-07T02:48:00Z"

    # Invalid datetime format
    with pytest.raises(ValidationError):
        HiredEmployeeSchema(
            id=101,
            name="John Doe",
            datetime="2021/11/07 02:48",
            department_id=5,
            job_id=96
        )

    # Empty name
    with pytest.raises(ValidationError):
        HiredEmployeeSchema(
            id=101,
            name=" ",
            datetime="2021-11-07T02:48:00Z",
            department_id=5,
            job_id=96
        )

# ============================================================================
# API Endpoint Integration Tests (using Mock DB)
# ============================================================================

@pytest.fixture
def mock_db():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    
    # Simple default fetchall/fetchone setups
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = {"count": 0}
    
    return conn, cursor

@pytest.fixture
def client(mock_db):
    conn, _ = mock_db
    def override_get_db():
        yield conn
    app.dependency_overrides[get_db] = override_get_db
    yield TestClient(app)
    app.dependency_overrides.clear()

def test_root_endpoint(client):
    response = client.get("/")
    assert response.status_code == 200
    # Returns HTML
    assert "text/html" in response.headers["content-type"]

def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy", "service": "catalog-api"}

def test_ingest_auth_failure(client):
    response = client.post(
        "/api/v1/ingest/departments",
        json={"rows": [{"id": 1, "department": "HR"}]}
    )
    # 403 Forbidden since X-API-Key header is missing
    assert response.status_code == 403

def test_ingest_table_not_found(client):
    response = client.post(
        "/api/v1/ingest/invalid_table_name",
        headers={"X-API-Key": "globant-secret"},
        json={"rows": [{"id": 1, "department": "HR"}]}
    )
    assert response.status_code == 404

def test_ingest_batch_size_exceeded(client):
    # Payload exceeding 1000 rows
    rows = [{"id": i, "department": f"Dept {i}"} for i in range(1001)]
    response = client.post(
        "/api/v1/ingest/departments",
        headers={"X-API-Key": "globant-secret"},
        json={"rows": rows}
    )
    assert response.status_code == 400
    assert "exceeds 1000 rows" in response.json()["detail"]

def test_ingest_departments_valid_data(client, mock_db):
    conn, cursor = mock_db
    response = client.post(
        "/api/v1/ingest/departments",
        headers={"X-API-Key": "globant-secret"},
        json={"rows": [
            {"id": 1, "department": "Product Management"},
            {"id": 2, "department": "Sales"}
        ]}
    )
    assert response.status_code == 200
    res = response.json()
    assert res["inserted"] == 2
    assert res["rejected"] == 0
    assert cursor.executemany.called

def test_ingest_hired_employees_with_fks(client, mock_db):
    conn, cursor = mock_db
    
    # Mocking active foreign keys in database
    # SELECT id FROM raw.departments
    # SELECT id FROM raw.jobs
    cursor.fetchall.side_effect = [
        [{"id": 5}, {"id": 6}],  # Departments
        [{"id": 96}]             # Jobs
    ]
    
    response = client.post(
        "/api/v1/ingest/hired_employees",
        headers={"X-API-Key": "globant-secret"},
        json={"rows": [
            # Valid employee (dept 5 exists, job 96 exists)
            {"id": 1, "name": "Harold", "datetime": "2021-11-07T02:48:00Z", "department_id": 5, "job_id": 96},
            # Invalid employee (dept 10 does not exist, job 96 exists)
            {"id": 2, "name": "Mimi", "datetime": "2021-11-07T02:48:00Z", "department_id": 10, "job_id": 96}
        ]}
    )
    assert response.status_code == 200
    res = response.json()
    assert res["inserted"] == 1
    assert res["rejected"] == 1
    assert "FK Violation" in res["errors"][0]["reason"]

def test_get_status_endpoint(client, mock_db):
    conn, cursor = mock_db
    cursor.fetchone.side_effect = [
        {"count": 12},  # departments
        {"count": 183}, # jobs
        {"count": 1929},# hired_employees
        {"count": 70}   # rejected_log
    ]
    response = client.get("/api/v1/status")
    assert response.status_code == 200
    res = response.json()
    assert res["departments"] == 12
    assert res["jobs"] == 183
    assert res["hired_employees"] == 1929
    assert res["rejected_log"] == 70

def test_get_metrics_hires_by_quarter(client, mock_db):
    conn, cursor = mock_db
    cursor.fetchall.return_value = [
        {"department": "HR", "job": "Recruiter", "q1": 1, "q2": 2, "q3": 0, "q4": 1}
    ]
    response = client.get("/api/v1/metrics/hires-by-quarter")
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["department"] == "HR"

def test_get_metrics_departments_above_mean(client, mock_db):
    conn, cursor = mock_db
    cursor.fetchall.return_value = [
        {"id": 5, "department": "Engineering", "hired": 500}
    ]
    response = client.get("/api/v1/metrics/departments-above-mean")
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["department"] == "Engineering"

def test_metrics_refresh_no_auth(client):
    response = client.post("/api/v1/metrics/refresh")
    assert response.status_code == 403

def test_metrics_refresh_empty_table(client, mock_db):
    conn, cursor = mock_db
    # Simulate empty hired_employees table
    cursor.fetchone.return_value = {"c": 0}
    response = client.post(
        "/api/v1/metrics/refresh",
        headers={"X-API-Key": "globant-secret"}
    )
    assert response.status_code == 400
    assert "empty" in response.json()["detail"]

def test_metrics_refresh_success(client, mock_db):
    conn, cursor = mock_db
    cursor.fetchone.side_effect = [
        {"c": 1685},   # source row count check
        {"c": 120},    # mart1 row count
        {"c": 5},      # mart2 row count
        {"mean": 140.4} # mean hires
    ]
    response = client.post(
        "/api/v1/metrics/refresh",
        headers={"X-API-Key": "globant-secret"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["source_rows_2021"] == 1685
    assert body["mart_departments_above_mean_rows"] == 5
