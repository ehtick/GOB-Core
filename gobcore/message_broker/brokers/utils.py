import argparse
import json
from gobcore.message_broker.brokers.broker import msg_broker


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--flush', help='flush the given queue')
    parser.add_argument('--publish', help='publish to a topic,key')
    parser.add_argument('--no-create', dest='create', action='store_false')
    parser.add_argument('--create', dest='create', action='store_true')
    parser.add_argument('--destroy', dest='destroy', action='store_true')
    parser.add_argument('--no-destroy', dest='destroy', action='store_false')
    args = parser.parse_args()
    with msg_broker.manager() as bm:
        print(' [*] Destroy exchanges')
        if args.destroy:
            bm.destroy_all()
            # bm.create_all()
        elif args.create:
            print(' [*] Create queues if not existing')
            bm.create_all()
    if args.flush:
        print(f'[*] flushing {args.flush}')
        with msg_broker.async_connection() as conn:
            for msg in conn.receive_msgs(args.flush):
                print(json.loads(msg))
    if args.publish:
        print(f'[*] publish {args.publish}')
        message = {"test": "message"}
        with msg_broker.async_connection() as conn:
            exchange, key = args.publish.split(',')
            conn.publish(exchange=exchange, key=key, msg=message)


if __name__ == '__main__':
    main()
