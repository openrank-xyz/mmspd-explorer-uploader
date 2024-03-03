import argparse
import asyncio
import importlib
import pkgutil
import sys
import traceback

import structlog

_logger = structlog.get_logger(__name__)


async def _collect_commands_into_parser(parser: argparse.ArgumentParser):
    cmds = parser.add_subparsers(dest='command', required=True)
    for mod_info in pkgutil.iter_modules(__path__):
        mod_name = mod_info.name
        if mod_name.startswith('__') and mod_name.endswith('__'):
            # Skip __init__, __main__, and future dunder names
            continue
        mod = importlib.import_module(f'{__name__}.{mod_name}')
        try:
            cmd_parser_args = dict(mod.CMD_PARSER_ARGS)
        except AttributeError:
            cmd_parser_args = {}
        cmd_parser_args.setdefault('help', "")
        cmd_parser = cmds.add_parser(mod_name, **cmd_parser_args)
        cmd_parser.set_defaults(func=mod.run)
        try:
            setup_parser = mod.setup_parser
        except AttributeError:
            pass
        else:
            await setup_parser(cmd_parser)


async def main_async():
    parser = argparse.ArgumentParser()
    await _collect_commands_into_parser(parser)
    args = parser.parse_args()
    await args.func(args)


def main():
    # noinspection PyBroadException
    try:
        return asyncio.run(main_async())
    except Exception:
        _logger.critical("mmspd-explorer-uploader finished abnormally",
                         exc=traceback.format_exc())
        return 1


if __name__ == '__main__':
    sys.exit(main())
