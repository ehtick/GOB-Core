import argparse
import json
from gobcore.message_broker.brokers.broker import msg_broker


def main(): # Noqa
    parser = argparse.ArgumentParser()
    parser.add_argument('--flush', help='flush the given queue')
    parser.add_argument('--number', help='Number of elements')
    parser.add_argument('--publish', help='publish to a exchange,key')
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
        print(f'[*] flushing {args.flush} {f"messages={args.number}" if args.number else ""}')
        with msg_broker.connection() as conn:
            kwargs = {'max_wait_time': 2}
            if args.number:
                kwargs['max_message_count'] = int(args.number)

            try:
                for m in conn.receive_msgs(args.flush, **kwargs):
                    print(json.loads(m))
            except KeyboardInterrupt:
                pass
    if args.publish:
        print(f'[*] publish {args.publish}')
        message = {"test": "message"}
        exchange, key, queue = args.publish.split(',')
        with msg_broker.connection() as conn:
            conn.publish(exchange=exchange, key=key, msg=message)
            exchange, key = args.publish.split(',')
            conn.publish_delayed(exchange=exchange, key=key, queue=queue, msg=message, delay_msec=1000)


if __name__ == '__main__':
    main()
