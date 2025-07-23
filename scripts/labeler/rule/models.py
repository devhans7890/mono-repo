from pydantic import BaseModel, Field, model_validator
from typing import List, Optional, Union, Literal
from datetime import datetime


class TimeCondition(BaseModel):
    field: str
    operator: Literal["between"]
    from_: datetime = Field(alias="from")
    to: datetime


class SearchCondition(BaseModel):
    field: str
    type: Literal["string", "long"]
    operator: str
    form: Optional[Literal["value", "field", "group", "previous"]] = None
    value: Optional[Union[str, int, float]] = None

    @model_validator(mode="after")
    def validate_condition(self):
        tp = self.type
        op = self.operator
        form = self.form
        val = self.value

        string_operators = {"==", "!=", "like", "not like", "exists", "not exists"}
        long_operators = {"==", "!=", ">", ">=", "<", "<=", "exists", "not exists"}

        if tp == "string" and op not in string_operators:
            raise ValueError(f"Invalid operator '{op}' for type 'string'")
        if tp == "long" and op not in long_operators:
            raise ValueError(f"Invalid operator '{op}' for type 'long'")

        if op in {"exists", "not exists"}:
            if form is not None or val is not None:
                raise ValueError(f"Operator '{op}' must not have form or value")
        else:
            if tp == "string" and form not in {"value", "field", "group", "previous"}:
                raise ValueError(f"Form '{form}' not valid for string type")
            if tp == "long" and form not in {"value", "field"}:
                raise ValueError(f"Form '{form}' not valid for long type")
            if val is None:
                raise ValueError("Value must be provided when form is used")
        return self


class SearchConditions(BaseModel):
    logic: Literal["and", "or"]
    conditions: List[Union["SearchCondition", "SearchConditions"]]


SearchConditions.model_rebuild()

class PersonalizationFilters(BaseModel):
    threshold: int
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None


class Step(BaseModel):
    id: str
    index_prefix: List[str]
    description: str
    base_fields: List[str]
    target_fields: List[str]
    operator: Literal["count", "sum", "average", "max", "min"]
    threshold: int
    top_n: int = 1

    time_condition: Optional[TimeCondition] = None
    search_conditions: Optional[SearchConditions] = None
    personalization_filters: Optional[PersonalizationFilters] = None


class AnalysisRule(BaseModel):
    id: str
    description: str
    level: str
    action: str
    notify: str
    retention: str
    extra_field: List[str]
    steps: List[Step]


class PersonalizationRule(BaseModel):
    id: str
    description: str
    base_field: str
    value_type: Literal["value", "field"]
    personalization_value: str
    retention_days: int
    threshold: int
    time_condition: Optional[TimeCondition] = None
    search_conditions: Optional[SearchConditions] = None


class RuleSet(BaseModel):
    analysis_rules: List[AnalysisRule]
    personalization_rules: List[PersonalizationRule]
