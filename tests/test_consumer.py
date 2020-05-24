from schloss import SchlossDispatcher, SchlossProducer, SchlossSession, SynchronousSchlossConsumer


async def test_run_consumer(url):
    message = b'test'

    def handler(session: SchlossSession):
        assert message == session.message

    dispatcher = SchlossDispatcher(
        handlers={
            'test': handler
        }
    )

    consumer = SynchronousSchlossConsumer(
        url=url,
        dispatcher=dispatcher,
        group_id='group',
        session_creator=SchlossSession.new
    )
    await consumer.start()
    producer = SchlossProducer(url=url)
    await producer.start()
    await producer.send('test', message)
    await producer.stop()
    await consumer.stop()
