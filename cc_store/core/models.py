"""
Data models for CC-Store.
"""

import datetime
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass, field


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


@dataclass
class FileRecord:
    """
    Record for a file containing domain data.
    
    Attributes:
        filepath: Path to the file
        start_offset: Starting offset for domain data in file
        end_offset: Ending offset for domain data in file
        record_count: Number of records for this domain in file
        timestamp: Unix timestamp when the file was written
        size_bytes: Size of the file in bytes
    """
    filepath: str
    start_offset: int
    end_offset: int
    record_count: int
    timestamp: int
    size_bytes: int
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary.
        
        Returns:
            Dictionary representation of this file record
        """
        return {
            'filepath': self.filepath,
            'start_offset': self.start_offset,
            'end_offset': self.end_offset,
            'record_count': self.record_count,
            'timestamp': self.timestamp,
            'size_bytes': self.size_bytes
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FileRecord':
        """
        Create from dictionary.
        
        Args:
            data: Dictionary to parse
            
        Returns:
            FileRecord instance
        """
        return cls(
            filepath=data['filepath'],
            start_offset=data['start_offset'],
            end_offset=data['end_offset'],
            record_count=data['record_count'],
            timestamp=data['timestamp'],
            size_bytes=data['size_bytes']
        )


@dataclass
class DomainMetadata:
    """
    Metadata for a domain across all files and dates.
    
    This class is designed to efficiently retrieve metadata about a domain,
    including record counts, time spans, and statistics.
    
    Attributes:
        domain: Domain name
        domain_hash_id: Numeric hash ID for this domain (0-999)
        total_records: Total number of records for this domain
        total_files: Total number of files containing this domain's data
        total_size_bytes: Total size of all files containing this domain's data
        min_date: Earliest date with data for this domain
        max_date: Latest date with data for this domain
        date_count: Number of unique dates with data for this domain
        stats: Additional statistics for this domain
        files: List of file records containing this domain's data
        count: Count of records for this domain
    """
    domain: str
    domain_hash_id: Optional[int] = None
    total_records: int = 0
    total_files: int = 0
    total_size_bytes: int = 0
    min_date: Optional[str] = None
    max_date: Optional[str] = None
    date_count: int = 0
    stats: Dict[str, Any] = field(default_factory=dict)
    files: List[Union[Dict, FileRecord]] = field(default_factory=list)
    count: int = 0
    
    def to_dict(self) -> Dict:
        """
        Convert to dictionary.
        
        Returns:
            Dictionary representation of this metadata
        """
        result = {
            'domain': self.domain,
            'total_records': self.total_records,
            'total_files': self.total_files,
            'total_size_bytes': self.total_size_bytes,
            'date_count': self.date_count,
            'stats': self.stats
        }
        
        if self.domain_hash_id is not None:
            result['domain_hash_id'] = self.domain_hash_id
            
        if self.min_date:
            result['min_date'] = self.min_date
            
        if self.max_date:
            result['max_date'] = self.max_date
            
        if self.files:
            # Convert FileRecord objects to dictionaries
            files_dicts = []
            for file in self.files:
                if isinstance(file, FileRecord):
                    files_dicts.append(file.to_dict())
                else:
                    files_dicts.append(file)
            result['files'] = files_dicts
            
        if self.count > 0:
            result['count'] = self.count
            
        return result
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'DomainMetadata':
        """
        Create from dictionary.
        
        Args:
            data: Dictionary to parse
            
        Returns:
            DomainMetadata instance
        """
        # Convert file dictionaries to FileRecord objects
        files = []
        for file_dict in data.get('files', []):
            try:
                files.append(FileRecord.from_dict(file_dict))
            except (KeyError, TypeError):
                # Fallback to raw dict if conversion fails
                files.append(file_dict)
        
        return cls(
            domain=data['domain'],
            domain_hash_id=data.get('domain_hash_id'),
            total_records=data.get('total_records', 0),
            total_files=data.get('total_files', 0),
            total_size_bytes=data.get('total_size_bytes', 0),
            min_date=data.get('min_date'),
            max_date=data.get('max_date'),
            date_count=data.get('date_count', 0),
            stats=data.get('stats', {}),
            files=files,
            count=data.get('count', 0)
        )


@dataclass
class FileMetadata:
    """
    Metadata for a single file in storage.
    
    Attributes:
        domain_id: Domain identifier
        date: Date string in YYYYMMDD format
        part_id: Part ID for this file
        file_path: Path to the file
        file_size_bytes: Size of the file in bytes
        records_count: Number of records in the file
        min_timestamp: Minimum timestamp in the file
        max_timestamp: Maximum timestamp in the file
        created_at: When this file was created
        checksum: Optional checksum of the file
        start_line: Starting line number for domain data in file
        end_line: Ending line number for domain data in file
    """
    domain_id: str
    date: str
    part_id: int
    file_path: str
    file_size_bytes: int
    records_count: int
    min_timestamp: Optional[datetime.datetime] = None
    max_timestamp: Optional[datetime.datetime] = None
    created_at: Optional[datetime.datetime] = None
    checksum: Optional[str] = None
    start_line: Optional[int] = None
    end_line: Optional[int] = None
    
    def to_dict(self) -> Dict:
        """
        Convert to dictionary.
        
        Returns:
            Dictionary representation of this metadata
        """
        result = {
            'domain_id': self.domain_id,
            'date': self.date,
            'part_id': self.part_id,
            'file_path': self.file_path,
            'file_size_bytes': self.file_size_bytes,
            'records_count': self.records_count,
        }
        
        if self.min_timestamp:
            result['min_timestamp'] = self.min_timestamp.timestamp()
        
        if self.max_timestamp:
            result['max_timestamp'] = self.max_timestamp.timestamp()
            
        if self.created_at:
            result['created_at'] = self.created_at.timestamp()
            
        if self.checksum:
            result['checksum'] = self.checksum
            
        if self.start_line is not None:
            result['start_line'] = self.start_line
            
        if self.end_line is not None:
            result['end_line'] = self.end_line
            
        return result
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'FileMetadata':
        """
        Create from dictionary.
        
        Args:
            data: Dictionary to parse
            
        Returns:
            FileMetadata instance
        """
        # Convert timestamp fields to datetime objects
        created_at = None
        if 'created_at' in data:
            created_at = datetime.datetime.fromtimestamp(data['created_at'])
            
        min_timestamp = None
        if 'min_timestamp' in data:
            min_timestamp = datetime.datetime.fromtimestamp(data['min_timestamp'])
            
        max_timestamp = None
        if 'max_timestamp' in data:
            max_timestamp = datetime.datetime.fromtimestamp(data['max_timestamp'])
            
        # Extract other fields
        start_line = data.get('start_line')
        end_line = data.get('end_line')
            
        return cls(
            domain_id=data['domain_id'],
            date=data['date'],
            part_id=data['part_id'],
            file_path=data['file_path'],
            file_size_bytes=data['file_size_bytes'],
            records_count=data['records_count'],
            min_timestamp=min_timestamp,
            max_timestamp=max_timestamp,
            created_at=created_at,
            checksum=data.get('checksum'),
            start_line=start_line,
            end_line=end_line
        ) 