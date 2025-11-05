from quixstreams import Application
import json


def main():
    app = Application(
        broker_address="127.0.01:19092, 127.0.0.1:29092, 127.0.0.1:39092",
        loglevel="DEBUG",
        consumer_group="wikipedia-consumer",
        auto_offset_reset="earliest"

    )
    new = 0
    edit = 0
    categorize = 0

    with app.get_consumer() as consumer:
        consumer.subscribe(["wikipedia-changes"])

        while True:
            msg = consumer.poll(1)

            if msg is None:
                print("Waiting...")
            elif msg.error() is not None:
                raise Exception(msg.error())
            else:
                key = msg.key().decode("utf8")
                value = json.loads(msg.value())
                change_type = value.get("type")
                offset = msg.offset()

                if change_type == "edit":
                    edit += 1
                elif change_type == "new":
                    new += 1
                elif change_type == "categorize":
                    categorize += 1
                

                print(f"{offset} {key} {change_type}")
                consumer.store_offsets(msg)
                return new, edit, categorize


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"Edits: {edit}, New: {new}, Categorizes: {categorize}")
        pass
