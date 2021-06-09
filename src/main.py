import asyncio
from argparse import ArgumentParser
from src.sofiax import SoFiAX


def main():
    """!SoFiAX entrypoint function.

    Initialise SoFiAX executor with config and parameter file, then
    run SoFiAX in the event loop.
    """
    # Parse arguments
    parser = ArgumentParser(description='SoFiAX standalone execution.')
    parser.add_argument('-c', '--config', dest='config', required=True,
                        help='SoFiAX Configuration file')
    parser.add_argument('-p', '--param', dest='param', nargs='+',
                        required=True, help='SoFiA parameter file')
    args = parser.parse_args()

    # Instantiate SoFiAX executor class
    sofiax = SoFiAX(args.config, args.param)

    # Run in event loop
    loop = asyncio.get_event_loop()
    loop.run_until_complete(sofiax.execute())
