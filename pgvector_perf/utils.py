from datetime import datetime
from typing import Text

import pytz


def gen_session_id(prefix: Text = "pgv"):
    return f'pgv{datetime.now(pytz.utc).strftime("%Y%m%d%H%M%S")}'
