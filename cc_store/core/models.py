"""
Data models for CC-Store.
"""

import datetime
from typing import Dict, List, Optional, Union, Any


class CCDocument:
    """Common Crawl document model."""
    
    def __init__(
        self,
        raw_warc_path: str,
        track_id: str,
        domain: str,
        url: str,
        status: int,
        response_header: Dict,
        date: int,
        content_length: int,
        content_charset: str,
        html: Optional[str] = None,
        content_hash: Optional[str] = None,
        remark: Optional[Dict] = None
    ):
        """
        Initialize a CCDocument instance.
        
        Args:
            raw_warc_path: Path to the raw WARC file
            track_id: Track ID
            domain: Domain name
            url: URL
            status: HTTP status code
            response_header: HTTP response headers
            date: Unix timestamp
            content_length: Content length in bytes
            content_charset: Content charset
            html: HTML content (optional)
            content_hash: Hash of the HTML content (optional)
            remark: Additional remarks (optional)
        """
        self.raw_warc_path = raw_warc_path
        self.track_id = track_id
        self.domain = domain
        self.url = url
        self.status = status
        self.response_header = response_header
        self.date = date
        self.content_length = content_length
        self.content_charset = content_charset
        self.html = html
        self.content_hash = content_hash
        self.remark = remark or {}
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "raw_warc_path": self.raw_warc_path,
            "track_id": self.track_id,
            "domain": self.domain,
            "url": self.url,
            "status": self.status,
            "response_header": self.response_header,
            "date": self.date,
            "content_length": self.content_length,
            "content_charset": self.content_charset,
            "html": self.html,
            "content_hash": self.content_hash,
            "remark": self.remark
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "CCDocument":
        """Create from dictionary."""
        return cls(
            raw_warc_path=data["raw_warc_path"],
            track_id=data["track_id"],
            domain=data["domain"],
            url=data["url"],
            status=data["status"],
            response_header=data["response_header"],
            date=data["date"],
            content_length=data["content_length"],
            content_charset=data["content_charset"],
            html=data.get("html"),
            content_hash=data.get("content_hash"),
            remark=data.get("remark", {})
        )


class DomainMetadata:
    """Metadata for a domain."""
    
    def __init__(
        self,
        domain_id: str,
        bucket_id: str,
        total_files: int,
        total_records: int,
        total_size_bytes: int,
        min_date: str,
        max_date: str,
        date_count: int,
        last_updated: datetime.datetime,
        stats: Dict = None
    ):
        """
        Initialize a DomainMetadata instance.
        
        Args:
            domain_id: Domain ID (domain name)
            bucket_id: Bucket ID
            total_files: Total number of files
            total_records: Total number of records
            total_size_bytes: Total size in bytes
            min_date: Minimum date (YYYYMMDD)
            max_date: Maximum date (YYYYMMDD)
            date_count: Number of distinct dates
            last_updated: Last updated timestamp
            stats: Additional statistics (optional)
        """
        self.domain_id = domain_id
        self.bucket_id = bucket_id
        self.total_files = total_files
        self.total_records = total_records
        self.total_size_bytes = total_size_bytes
        self.min_date = min_date
        self.max_date = max_date
        self.date_count = date_count
        self.last_updated = last_updated
        self.stats = stats or {}
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "domain_id": self.domain_id,
            "bucket_id": self.bucket_id,
            "total_files": self.total_files,
            "total_records": self.total_records,
            "total_size_bytes": self.total_size_bytes,
            "min_date": self.min_date,
            "max_date": self.max_date,
            "date_count": self.date_count,
            "last_updated": self.last_updated.isoformat(),
            "stats": self.stats
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "DomainMetadata":
        """Create from dictionary."""
        # Parse last_updated from string to datetime
        if isinstance(data["last_updated"], str):
            last_updated = datetime.datetime.fromisoformat(data["last_updated"])
        else:
            last_updated = data["last_updated"]
            
        return cls(
            domain_id=data["domain_id"],
            bucket_id=data["bucket_id"],
            total_files=data["total_files"],
            total_records=data["total_records"],
            total_size_bytes=data["total_size_bytes"],
            min_date=data["min_date"],
            max_date=data["max_date"],
            date_count=data["date_count"],
            last_updated=last_updated,
            stats=data.get("stats", {})
        )


class FileMetadata:
    """Metadata for a data file."""
    
    def __init__(
        self,
        domain_id: str,
        date: str,
        part_id: int,
        file_path: str,
        file_size_bytes: int,
        records_count: int,
        min_timestamp: datetime.datetime,
        max_timestamp: datetime.datetime,
        created_at: datetime.datetime,
        checksum: str,
        file_format_version: str = "1.0",
        compression: str = "snappy",
        statistics: Dict = None
    ):
        """
        Initialize a FileMetadata instance.
        
        Args:
            domain_id: Domain ID (domain name)
            date: Date string (YYYYMMDD)
            part_id: Part ID
            file_path: Path to the file
            file_size_bytes: File size in bytes
            records_count: Number of records
            min_timestamp: Minimum timestamp
            max_timestamp: Maximum timestamp
            created_at: Creation timestamp
            checksum: File checksum
            file_format_version: File format version (optional)
            compression: Compression algorithm (optional)
            statistics: File statistics (optional)
        """
        self.domain_id = domain_id
        self.date = date
        self.part_id = part_id
        self.file_path = file_path
        self.file_size_bytes = file_size_bytes
        self.records_count = records_count
        self.min_timestamp = min_timestamp
        self.max_timestamp = max_timestamp
        self.created_at = created_at
        self.checksum = checksum
        self.file_format_version = file_format_version
        self.compression = compression
        self.statistics = statistics or {}
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "domain_id": self.domain_id,
            "date": self.date,
            "part_id": self.part_id,
            "file_path": self.file_path,
            "file_size_bytes": self.file_size_bytes,
            "records_count": self.records_count,
            "min_timestamp": self.min_timestamp.isoformat(),
            "max_timestamp": self.max_timestamp.isoformat(),
            "created_at": self.created_at.isoformat(),
            "checksum": self.checksum,
            "file_format_version": self.file_format_version,
            "compression": self.compression,
            "statistics": self.statistics
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "FileMetadata":
        """Create from dictionary."""
        # Parse timestamps from strings to datetime
        if isinstance(data["min_timestamp"], str):
            min_timestamp = datetime.datetime.fromisoformat(data["min_timestamp"])
        else:
            min_timestamp = data["min_timestamp"]
            
        if isinstance(data["max_timestamp"], str):
            max_timestamp = datetime.datetime.fromisoformat(data["max_timestamp"])
        else:
            max_timestamp = data["max_timestamp"]
            
        if isinstance(data["created_at"], str):
            created_at = datetime.datetime.fromisoformat(data["created_at"])
        else:
            created_at = data["created_at"]
            
        return cls(
            domain_id=data["domain_id"],
            date=data["date"],
            part_id=data["part_id"],
            file_path=data["file_path"],
            file_size_bytes=data["file_size_bytes"],
            records_count=data["records_count"],
            min_timestamp=min_timestamp,
            max_timestamp=max_timestamp,
            created_at=created_at,
            checksum=data["checksum"],
            file_format_version=data.get("file_format_version", "1.0"),
            compression=data.get("compression", "snappy"),
            statistics=data.get("statistics", {})
        )


@dataclass
class HTMLContentMetadata:
    """
    Metadata for stored HTML content.
    """
    content_hash: str
    compressed_size: int
    original_size: int
    first_seen_date: str  # YYYYMMDD format
    last_seen_date: str  # YYYYMMDD format
    reference_count: int
    compression_method: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "content_hash": self.content_hash,
            "compressed_size": self.compressed_size,
            "original_size": self.original_size,
            "first_seen_date": self.first_seen_date,
            "last_seen_date": self.last_seen_date,
            "reference_count": self.reference_count,
            "compression_method": self.compression_method
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'HTMLContentMetadata':
        """Create from dictionary representation."""
        return cls(**data)


@dataclass
class HTMLReference:
    """
    Reference mapping between URLs and HTML content.
    """
    domain: str
    url: str
    content_hash: str
    date: str  # YYYYMMDD format
    version: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "domain": self.domain,
            "url": self.url,
            "content_hash": self.content_hash,
            "date": self.date,
            "version": self.version,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'HTMLReference':
        """Create from dictionary representation."""
        return cls(**data) 