from jinja2 import Template

def render_jinja2(sql_template: str, params: dict) -> str:
    return Template(sql_template).render(**params)