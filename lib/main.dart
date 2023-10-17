import 'package:flutter/material.dart';
import 'package:kafkabr/kafka.dart';

void main() {
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {

  const MyApp({super.key});

  // This widget is the root of your application.
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      title: 'Flutter Demo',
      theme: ThemeData(
        primarySwatch: Colors.blue,
      ),
      home: const MyHomePage(title: 'Flutter Demo Home Page'),
    );
  }
}

class MyHomePage extends StatefulWidget {
  const MyHomePage({super.key, required this.title});
  final String title;

  @override
  State<MyHomePage> createState() => _MyHomePageState();
}

class _MyHomePageState extends State<MyHomePage> {
  String variaveTop = "";

  @override
  void initState() {
    // TODO: implement initState
    super.initState();
    consumindoTopic();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text(widget.title),
      ),
      body: Center(
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: <Widget>[
            const Text(
              'You have pushed the button this many times:',
            ),
            Text(
              '$variaveTop',
              style: Theme.of(context).textTheme.headlineMedium,
            ),
          ],
        ),
      ),
    );
  }

  Future<void> consumindoTopic() async {
    var host = new ContactPoint('192.168.0.107', 9092);
    var session = new KafkaSession([host]);
    var group = new ConsumerGroup(session, 'parcelaconta_consumer_group');
    final Map<String, Set<int>> topics = {
      'mil': {0},
    };

    topics.forEach((key, value) {
      value.forEach((element) {
        print(element);
      });
    });


    var consumer = new Consumer(session, group, topics, 10, 1);
    await for (BatchEnvelope envelope in consumer.batchConsume(50)) {
      envelope.items.forEach((MessageEnvelope envelope) {
        List<int> charCodes = envelope.message.value;
        setState(() {
          variaveTop = new String.fromCharCodes(charCodes);
        });
      });
      // Assuming that messages were produces by Producer from previous example.
      print(envelope);
      envelope.commit('metadata'); // Important.
    }
    session.close();
  }
}
