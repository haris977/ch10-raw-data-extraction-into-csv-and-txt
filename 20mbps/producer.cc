#include<fstream>
#include<thread>
#include<chrono>
#include<librdkafka/rdkafkacpp.h>
#define HEADER_SIZE 24
#define TARGET_BPS 2500000
using namespace std;

int main(){
    ifstream f("data.ch10",ios::binary);
    string errstr;
    Rdkafka::Conf * conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)
    conf->set("bootstrap.servers","localhost:9092",errstr);
    conf->set("linger.ms", "5", errstr);
    conf->set("batch.size", "65536", errstr);
    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);

    long sent = 0;
    auto start = chrono::steady_clock::now();

    while (f){
        char sync[2];
        f.read(sync,2);
        if (!f || sync[0]!=0x25 || sync[1]!= (char)0xEB) continue;
        char header[HEADER_SIZE];
        header[0] = sync[0];
        header[1] = sync[1];

        f.read(header + 2, HEADER_SIZE-2);

        uint32_t plen = *(uint32_t *)(header +4);
        
        string packet(header,HEADER_SIZE);
        packet.resize(plen);
        f.read(&packet[HEADER_SIZE],plen-HEADER_SIZE);

        producer->produce(
            "ch10.raw.frames",
            RdKafka::Producer::RK_MSG_COPY,
            packet.data(), packet.size(),
            nullptr, 0, 0, nullptr
        );

        sent += packet.size();
        auto now = chrono::steady_clock::now();
        double eclapsed = chrono::duration<double>(now-start).count();

        if (eclapsed>0 && sent/eclapsed> TARGET_BPS){
            this_thread::sleep_for(chrono::milliseconds(500));
        }
    }
    producer->flush(5000);
    delete producer;
    delete conf;
    return 0;
}
