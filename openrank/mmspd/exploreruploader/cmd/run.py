import argparse
import asyncio
import contextlib
import json
import pathlib
import tempfile
import traceback
import zipfile
from argparse import ArgumentParser
from datetime import datetime

import aioboto3
import structlog

_logger: structlog.BoundLoggerBase = structlog.get_logger(__name__)


async def setup_parser(parser: ArgumentParser):
    parser.add_argument('--aws-profile', default='default',
                        help="""AWS CLI profile name""")
    parser.add_argument('--aws-region',
                        help="""AWS region name""")
    parser.add_argument('--s3-uploaders', metavar='NUM', type=int, default=10,
                        help="""number of parallel S3 uploader tasks""")
    parser.add_argument('directory', metavar='DIRECTORY', type=pathlib.Path,
                        help="""output directory to scan""")
    parser.add_argument('s3_bucket', metavar='BUCKET',
                        help="""AWS S3 bucket name""")
    parser.add_argument('indexer_cache', type=pathlib.Path,
                        help="""cache CSV file for /indexer-scores""")


def parse_timestamp(s: str) -> datetime:
    try:
        return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%f%z')
    except ValueError:
        return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S%z')


def rm_rf(path: pathlib.Path):
    if path.is_dir() and not path.is_symlink():
        for child in path.iterdir():
            rm_rf(child)
        path.rmdir()
    else:
        path.unlink(missing_ok=True)


async def upload_to_s3(args: argparse.Namespace, worker_index: int,
                       queue: asyncio.Queue):
    logger = _logger
    session = aioboto3.Session(profile_name=args.aws_profile,
                               region_name=args.aws_region)
    my_name = f'uploader-{worker_index}'
    logger = logger.bind(worker=my_name)
    bucket = None
    async with contextlib.AsyncExitStack() as stack:
        while True:
            match await queue.get():
                case None:
                    await queue.put(None)
                    logger.info("finished")
                    return
                case (path, key):
                    # noinspection PyBroadException
                    try:
                        if bucket is None:
                            s3 = await stack.enter_async_context(
                                session.resource('s3'))
                            bucket = await s3.Bucket(args.s3_bucket)
                        await bucket.upload_file(f'{path}', key)
                        logger.info("uploaded", path=path, key=key,
                                    bucket=args.s3_bucket)
                    except Exception:
                        logger.error("upload failed", path=path, key=key,
                                     bucket=args.s3_bucket,
                                     exception=traceback.format_exc())


async def run(args):
    # TODO(ek): Use some sort of filesystem monitor
    manifest_by_ts = dict[datetime, dict]()
    timestamps_by_epoch = dict[datetime, set[int]]()
    directory: pathlib.Path = args.directory
    while True:
        _logger.info("starting a run")
        s3_upload_queue = asyncio.Queue(args.s3_uploaders * 4)
        uploaders = [
            asyncio.create_task(
                upload_to_s3(args, i, s3_upload_queue))
            for i in range(args.s3_uploaders)
        ]
        tmpdir = pathlib.Path(tempfile.mkdtemp())
        try:
            for path in directory.iterdir():
                logger = _logger.bind(path=path)
                match path.suffix:
                    case '.json':
                        try:
                            ts0 = int(path.stem)
                        except ValueError:
                            continue
                        if ts0 in manifest_by_ts:
                            continue
                        try:
                            with path.open() as f:
                                manifest = json.load(f)
                        except (OSError, json.JSONDecodeError):
                            _logger.error("cannot load manifest",
                                          exc=traceback.format_exc())
                            continue
                        try:
                            epoch = parse_timestamp(manifest['epoch'])
                            ts = parse_timestamp(manifest['issuanceDate'])
                            ets = parse_timestamp(manifest['effectiveDate'])
                        except KeyError:
                            _logger.error("invalid manifest",
                                          exc=traceback.format_exc())
                            continue
                        except ValueError:
                            _logger.error(
                                "invalid epoch/issuance/effective date",
                                exc=traceback.format_exc())
                            continue
                        assert round(ts.timestamp() * 1000) == ts0
                        with zipfile.ZipFile(path.with_suffix('.zip')) as z:
                            zipdir = tmpdir / path.stem
                            z.extractall(zipdir)
                            for path2 in zipdir.iterdir():
                                if path2.is_file():
                                    await s3_upload_queue.put((f'{path2}',
                                                               f'files/{ts0}/{path2.relative_to(tmpdir)}'))
                        manifest_by_ts[ts0] = manifest
                        timestamps_by_epoch.setdefault(epoch, set()).add(ts0)
            try:
                max_epoch = max(timestamps_by_epoch.keys())
            except ValueError:
                continue
            timestamps = timestamps_by_epoch[max_epoch]
            list_ = [str(ts) for ts in sorted(timestamps)]
            list_json = tmpdir / 'list.json'
            with list_json.open('w') as f:
                json.dump(list_, f)
            await s3_upload_queue.put(
                (f'{list_json}', 'api/scores/list.json'))
            await s3_upload_queue.put(
                (f'{args.indexer_cache}', 'api/scores/indexer-scores'))
            _logger.info("finished a run")
        finally:
            await s3_upload_queue.put(None)
            await asyncio.gather(*uploaders, return_exceptions=True)
            _logger.info("all uploads finished")
            rm_rf(tmpdir)
        await asyncio.sleep(10)
