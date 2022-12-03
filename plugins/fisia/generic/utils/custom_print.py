import logging


# -------------------------------------------------------------
def default_logging(*values: object, sep: str = " ", end: str = ""):
    logging.info(
        " " + (sep or " ").join([str(value) for value in values]) + (end or "")
    )
