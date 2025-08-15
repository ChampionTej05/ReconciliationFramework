# recon/core/config.py
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Literal

class SanitizeCfg(BaseModel):
    rename: Dict[str, str] = Field(default_factory=dict)
    select: List[str] = Field(default_factory=list)
    normalize: Dict[str, List[str] | bool] = Field(default_factory=dict)

class ReadCfg(BaseModel):
    path: str
    delimiter: str = ","
    encoding: str = "utf-8"
    dtypes: Dict[str, str] = Field(default_factory=dict)
    header: Optional[int] = 0
    sanitize: Optional[SanitizeCfg] = None
    prefilter: List[Dict] = Field(default_factory=list)

class AggregateSpec(BaseModel):
    group_by: List[str] = Field(default_factory=list)
    metrics: Dict[str, Dict[str, str]] = Field(default_factory=dict)

# Typed wrapper for A/B aggregate
class AggregateCfg(BaseModel):
    A: Optional[AggregateSpec] = None
    B: Optional[AggregateSpec] = None

class JoinCfg(BaseModel):
    keys: List[str] = Field(default_factory=list)
    key_name: Optional[str] = None
    type: Literal["inner","left","right","outer"] = "outer"

class ReconNumeric(BaseModel):
    column: str
    comparator: Literal["relative","absolute","rounded","exact"] = "relative"
    tol_pct: Optional[float] = None
    tol_abs: Optional[float] = None
    round: Optional[int] = None
    min_base: float = 1e-8

# Wrapper for reconcile section with numeric list
class ReconcileCfg(BaseModel):
    numeric: List[ReconNumeric] = Field(default_factory=list)

class ReportOutputs(BaseModel):
    dir: str = "out"
    formats: List[str] = Field(default_factory=lambda: ["csv"])

# Typed wrapper for report select
class ReportSelect(BaseModel):
    keys: List[str] = Field(default_factory=list)

class ReportCfg(BaseModel):
    outputs: ReportOutputs
    dataset_names: Dict[str, str] = Field(default_factory=dict)
    select: Optional[ReportSelect] = None

class DrillLevel(BaseModel):
    add: List[str] = Field(default_factory=list)
    A_add: List[str] = Field(default_factory=list)
    B_add: List[str] = Field(default_factory=list)
    remove: List[str] = Field(default_factory=list)
    A_remove: List[str] = Field(default_factory=list)
    B_remove: List[str] = Field(default_factory=list)

class DrilldownCfg(BaseModel):
    enabled: bool = False
    strategy: Literal["add","remove"] = "add"
    levels: List[DrillLevel] = Field(default_factory=list)

class JobCfg(BaseModel):
    name: str
    backend: Literal["pandas"] = "pandas"
    join_type: str = "outer"
    timezone: str = "Asia/Kolkata"

# Typed wrapper for inputs to allow attribute access (cfg.inputs.A, cfg.inputs.B)
class InputsCfg(BaseModel):
    A: ReadCfg
    B: ReadCfg

class RootCfg(BaseModel):
    job: JobCfg
    inputs: InputsCfg  # provides cfg.inputs.A and cfg.inputs.B
    filters: Dict[str, List[Dict]] = Field(default_factory=dict)
    aggregate: Optional[AggregateCfg] = None
    join: JoinCfg
    reconcile: Optional[ReconcileCfg] = None
    report: ReportCfg
    drilldown: Optional[DrilldownCfg] = None