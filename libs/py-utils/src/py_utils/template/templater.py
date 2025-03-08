from typing import Union
import datetime
from jinja2 import Template

def ds_filter(value: Union[datetime.date, datetime.time, None]) -> Union[str, None]:
    """Date filter."""
    if value is None:
        return None
    return value.strftime("%Y-%m-%d")

def ds_nodash_filter(value: Union[datetime.date, datetime.time, None]) -> Union[str, None]:
    """Date filter without dashes."""
    if value is None:
        return None
    return value.strftime("%Y%m%d")

def ts_filter(value: Union[datetime.date, datetime.time, None]) -> Union[str, None]:
    """Timestamp filter."""
    if value is None:
        return None
    return value.isoformat()

def ts_nodash_filter(value: Union[datetime.date, datetime.time, None]) -> Union[str, None]:
    """Timestamp filter without dashes."""
    if value is None:
        return None
    return value.strftime("%Y%m%dT%H%M%S")

def ts_nodash_with_tz_filter(value: Union[datetime.date, datetime.time, None]) -> Union[str, None]:
    """Timestamp filter with timezone."""
    if value is None:
        return None
    return value.isoformat().replace("-", "").replace(":", "")

FILTERS = {
    "ds": ds_filter,
    "ds_nodash": ds_nodash_filter,
    "ts": ts_filter,
    "ts_nodash": ts_nodash_filter,
    "ts_nodash_with_tz": ts_nodash_with_tz_filter,
}

def render_template_body_json(template: str) -> str:
    for key, value in template.items():
        start = value.find('{{') + 2
        end = value.find('}}')
        if start == -1 or end == -1:
            return template
        filter_name = value[start:end].strip()
        if filter_name in FILTERS:
            value_template = Template(value)
            current_value = datetime.date.today()
            knights = {
                f"{filter_name}": FILTERS[filter_name](current_value)
            }
            filtered_value = value_template.render(knights)
            template[key] = filtered_value
    return template

# Example usage
# Example usage
body_json = {
    "date_run": "ahihi-{{ds}}-{{ds}}",
    "query": "select * from a where date = '{{ds}}'"
}
rendered_template = render_template_body_json(body_json)
print(rendered_template['date_run'])
print(rendered_template['query'])