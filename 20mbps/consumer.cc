#include<fstream>
#include<librdkafka/rdkafkacpp.h>
#include<iostream>
#include<vector>
#include<string>
#define HEADER_SIZE 24
using namespace std;

int main(){

    string errstr;
    RdKafka::Conf * conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.server","localhost:9092",errstr);
    conf->set("group_id","c20",errstr);
    conf->set("auto.offset.reset","earliest",errstr);
    conf->set("fetch.max.bytes", "5000000", errstr);

    RdKafka::Consumer * consumer = RdKafka::Consumer::create(conf, errstr);
    consumer->subscribe({"ch10.raw.frames"});
    ofstream out ("dump_20mbps",ios::binary);
    vector<string> buffer;
    size_t bufsize = 0;
    while (true){
        RdKafka::Message * msg = consumer->consume(1000);
        if (msg->err() == RdKafka::ERR_NO_ERROR){
            buffer.emplace_back((char* )msg->payload(),msg->len());
            bufsize += msg->len();

            if (bufsize>=65536){
                for (auto & b: buffer){
                    out.write(b.data(),b.size());
                }
                out.flush();
                buffer.clear();
                bufsize =0;
            }
        }
        delete msg;
    }

    return 0;
}