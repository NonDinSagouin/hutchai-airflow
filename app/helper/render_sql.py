from jinja2 import Environment, FileSystemLoader

def render_sql(template_path: str, params: dict) -> str:
    """
    Charge un fichier SQL et applique des paramètres Jinja.
    """
    env = Environment(
        loader=FileSystemLoader("/"),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
    )

    template = env.get_template(template_path)
    return template.render(**params)
