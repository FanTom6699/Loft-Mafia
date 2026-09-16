"""Split handler package.

All handler functions are extracted from the original handlers.py.
The package keeps a shared runtime context and re-exports the same public
handler names so existing imports continue to work.
"""

from . import _context as _context

from . import common as _common
from . import lobby as _lobby
from . import game as _game
from . import day as _day
from . import profile as _profile
from . import settings as _settings
from . import tickets as _tickets
from . import night as _night
from . import callbacks as _callbacks

# Make every extracted function able to resolve helpers from the other modules
# at runtime, while keeping one shared router/repository/storage instance.
_modules = [_common, _lobby, _game, _day, _profile, _settings, _tickets, _night, _callbacks]
_shared = {}
for _module in _modules:
    for _name, _value in vars(_module).items():
        if not _name.startswith('__'):
            _shared[_name] = _value

for _module in _modules:
    _module.__dict__.update(_shared)

globals().update(_shared)

__all__ = [name for name in globals() if not name.startswith('_')]
