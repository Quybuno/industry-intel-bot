from datetime import datetime
import uuid
from sqlalchemy import (
    Column,
    String,
    Text,
    DateTime,
    Boolean,
    Integer,
    Numeric,
    JSON,
    func,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, ARRAY
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


def gen_uuid():
    return str(uuid.uuid4())


class Source(Base):
    __tablename__ = 'sources'
    id = Column(String, primary_key=True)
    type = Column(String, nullable=False)
    url_or_query = Column(Text, nullable=False)
    tier = Column(Integer, nullable=True)
    industries = Column(JSON, nullable=True)
    enabled = Column(Boolean, default=True)
    last_checked = Column(DateTime, nullable=True)


class Article(Base):
    __tablename__ = 'articles'
    id = Column(String, primary_key=True, default=gen_uuid)
    canonical_url = Column(Text, unique=True, index=True, nullable=False)
    content_hash = Column(String, index=True, nullable=True)
    source_id = Column(String, nullable=True)
    source_type = Column(String, nullable=True)
    title = Column(Text, nullable=True)
    snippet = Column(Text, nullable=True)
    full_text = Column(Text, nullable=True)
    published_at = Column(DateTime, nullable=True)
    first_seen_at = Column(DateTime, default=func.now())
    status = Column(String, default='raw')
    industry_tags = Column(JSON, nullable=True)
    rejection_reason = Column(String, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class ArticleScore(Base):
    __tablename__ = 'article_scores'
    id = Column(String, primary_key=True, default=gen_uuid)
    article_id = Column(String, nullable=False, index=True)
    model_name = Column(String, nullable=False)
    prompt_version = Column(String, nullable=False)
    tier = Column(String, nullable=True)
    credibility = Column(Integer, nullable=True)
    importance = Column(Integer, nullable=True)
    depth = Column(Integer, nullable=True)
    practicality = Column(Integer, nullable=True)
    composite_score = Column(Numeric(4, 2), nullable=True)
    summary_vi = Column(JSON, nullable=True)
    why_it_matters_vi = Column(Text, nullable=True)
    confidence = Column(String, nullable=True)
    is_breaking = Column(Boolean, default=False)
    raw_response = Column(JSON, nullable=True)
    latency_ms = Column(Integer, nullable=True)
    scored_at = Column(DateTime, default=func.now())


class JobRun(Base):
    __tablename__ = 'job_runs'
    id = Column(String, primary_key=True, default=gen_uuid)
    job_name = Column(String, nullable=False)
    started_at = Column(DateTime, default=func.now())
    finished_at = Column(DateTime, nullable=True)
    status = Column(String, nullable=False, default='running')
    items_processed = Column(Integer, default=0)
    items_failed = Column(Integer, default=0)
    error_summary = Column(Text, nullable=True)
    run_metadata = Column(JSON, nullable=True)


class SourceHealth(Base):
    __tablename__ = 'source_health'
    source_id = Column(String, primary_key=True)
    last_success_at = Column(DateTime, nullable=True)
    consecutive_failures = Column(Integer, default=0)
    last_error = Column(Text, nullable=True)
