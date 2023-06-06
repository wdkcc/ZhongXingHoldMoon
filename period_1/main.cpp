#include<iostream>
#include<fstream>
#include<stdio.h>
#include <sys/types.h> 
#include <dirent.h>
#include<string.h>
#include <ctime>
#include<algorithm>
#include<limits.h>
#include<map>
// #include<ctime>
#include<queue>

using namespace std;


const char ROOT[] = "../data/";
int cur_time=0, cur_fi=0;

typedef struct Flow{
    int id;
    int bw; // bandwidth
    int et; //enter_time
    int nt;
    bool operator < (const Flow &f) const{
        return this->bw < f.bw;
    }
}Flow;

typedef struct{
    int id;
    int bw;
}Port;

typedef struct{
    int fid;
    int pid;
    int st; // seed_time
}Result;

typedef struct {
    Flow *data = (Flow*)malloc(sizeof(Flow)*1000000);
    int length = 0;
}FlowArr;

typedef struct{
    Port data[10000];
    int length = 0;
}PortArr;

typedef struct {
    Result *data = (Result*)malloc(sizeof(Result)*1000000);
    int length = 0;
}ResultArr;

struct FlowLog{
    Flow flow;
    int end_time;
    bool operator < (const FlowLog &f) const{
        return this->end_time > f.end_time;
    }
};

class PortQueue{
public:
    int id;
    int bw;
    priority_queue<FlowLog> heap;
    queue<Flow> buffer_queue;
    int cur_available_bandwidth;

    PortQueue(int pid, int bandwidth){
        id = pid;
        bw = bandwidth;
        cur_available_bandwidth = bw;
    }
    bool operator < (const PortQueue &f) const{
        return this->cur_available_bandwidth < f.cur_available_bandwidth; // <是大根堆
    }
    // 将流记录压入端口堆中
    void put_port_heap(FlowLog flowlog){
        heap.push(flowlog);
        cur_available_bandwidth -= flowlog.flow.bw;
    }
    // 更新端口堆区的流数据
    void update_port_heap(){
        while (!heap.empty() && heap.top().end_time <= cur_time){
            FlowLog flowlog = heap.top();
            heap.pop();
            cur_available_bandwidth += flowlog.flow.bw;
        }
    }
    // 更新缓存区的流数据
    void update_buffer_queue(){
        if(buffer_queue.empty())
            return;
        while (!buffer_queue.empty() && buffer_queue.front().bw <= cur_available_bandwidth){
            Flow flow = buffer_queue.front();
            buffer_queue.pop();
            put_port_heap(FlowLog{flow, flow.nt+cur_time});
        }
    }
    
    void push(Flow flow){
        // 缓存区为空且当前流的带宽能够被端口容纳，那么就直接将流放入端口堆中
        if (buffer_queue.empty() && flow.bw <= cur_available_bandwidth)
            put_port_heap(FlowLog{flow, flow.nt+cur_time});
        else
            // 流放入缓存区
            buffer_queue.push(flow);
    }
    
};

struct FlowNode{
    Flow data;
    FlowNode * next;
};

class DirectInsertQueue{
private:
    FlowNode* head;
    FlowNode* end;
    bool (*cmp)(Flow, Flow);
public:
    DirectInsertQueue(bool (*cmp_fuc)(Flow, Flow)){
        head = (FlowNode*)malloc(sizeof(FlowNode));
        head->next = NULL;
        end = head;
        cmp = cmp_fuc;
    }
    Flow top(){
        return head->next->data;
    }
    void push(Flow f){
        FlowNode *node = (FlowNode*)malloc(sizeof(FlowNode));
        node->data = f;
        node->next = NULL;
        FlowNode* temhead = head;
        while (temhead->next != NULL){
            if (!cmp(node->data, temhead->next->data))
                temhead = temhead->next;
            else
                break;
        }
        if (temhead->next == NULL)
            end = node;
        node->next = temhead->next;
        temhead->next = node;

    }
    // 搜索满足能够容纳得了的流，找到了就返回通过引用传递返回该流，否则返回true
    bool search_and_pop(int available_bw, Flow &f){
        FlowNode* temhead = head;
        while (temhead->next != NULL){
            if (temhead->next->data.bw <= available_bw){
                if (temhead->next->next == NULL)
                    end = temhead->next;
                f = temhead->next->data;
                temhead->next = temhead->next->next;
                return true;
            }else
                temhead = temhead->next;
        }
        return false;
    }

    bool empty(){
        if (head->next == NULL)
            return true;
        return false;
    }

    Flow end_flow(){
        return end->data;
    }    
    void print_queue(){
        FlowNode* temhead = head;
        while (temhead->next != NULL){
            cout<< temhead->next->data.bw << " " << temhead->next->data.nt << "\n";
            temhead = temhead->next;
        }
        cout << '\n';
    }
};

bool cmp_by_bw_nt(Flow f1, Flow f2){
    // if (f1.bw != f2.bw)
    //     return f1.bw > f2.bw; // <是递增 >是递减
    // return f1.nt>f2.nt;
    if (f1.nt != f2.nt)
        return f1.nt > f2.nt; // <是递增 >是递减
    return f1.bw>f2.bw;
}

void create_port_queues(vector<Port> ports, vector<PortQueue> &port_queues){
    for (int i=0; i<ports.size(); i++)
        port_queues.push_back(PortQueue(ports[i].id, ports[i].bw));
}
  
bool read_data(vector<Port>&ports, vector<Flow>&flows, int dirNum){
    char flow_path[50], port_path[50];
    sprintf(flow_path, "%s%d/flow.txt", ROOT, dirNum);
    sprintf(port_path, "%s%d/port.txt", ROOT, dirNum);
    
	ifstream read_file;
    read_file.open(flow_path, ios::in);
    if (!read_file.is_open())
        return false;
    read_file.ignore(100, '\n');
    
    while (!read_file.eof()){
        char t;
        Flow flow{-1, 0, 0, 0};
        read_file >> flow.id >> t >> flow.bw >> t >> flow.et >> t >> flow.nt;
        if (flow.id == -1)
            break;
        flows.push_back(flow);
    }
    read_file.close();

    read_file.open(port_path, ios::in);
    if (!read_file.is_open())
        return false;
    read_file.ignore(100, '\n');
    while (!read_file.eof()){
        char t;
        Port port{-1, 0};
        read_file >> port.id >> t >> port.bw;
        if (port.id == -1)
            break;
        ports.push_back(port);
    }
    read_file.close();
    return true;
}

int cmp_by_et(Flow f1, Flow f2){
    return f1.et < f2.et;
}
int cmp_by_flow_bw(Flow f1, Flow f2){
    return f1.bw < f2.bw;
}

struct PortResBw{
    int pi;
    int res;
    vector<int> indexs;
    bool operator < (const int r) const{
        return this->res > r;
    }
};

DirectInsertQueue cur_flows = DirectInsertQueue(cmp_by_bw_nt);
// 得到新进入设备的流
void get_new_flows(vector<Flow> flows){
    if (cur_fi < flows.size() && cur_time < flows[cur_fi].et)
        cur_time = flows[cur_fi].et;
    for (;cur_fi<flows.size(); cur_fi++)
        if (flows[cur_fi].et <= cur_time)
            cur_flows.push(flows[cur_fi]);
        else
            break;
}

map<int, int> deleted_flows;

bool search_flow_in_deleted_flows(int fid){
    auto iter = deleted_flows.find(fid);
    if (iter != deleted_flows.end())
        return true;
    else
        return false;
}

ofstream debug_output;
bool sort_by_available_bw(PortQueue pq1, PortQueue pq2){
    return pq1.cur_available_bandwidth > pq2.cur_available_bandwidth;
}

void print_port_cur_available_bw(vector<PortQueue> &port_queues){
    for (int i=0; i<port_queues.size(); i++)
        cout << port_queues[i].cur_available_bandwidth << " ";
    cout << '\n';
}
// 输入当前时刻的流数据，端口队列。将当前时刻的流分配给对应端口
void flow_assign(vector<PortQueue> &port_queues, vector<Result> &results){
    // 更新当前端口带宽
    for (int i=0; i<port_queues.size(); i++)
        port_queues[i].update_port_heap();
    make_heap(port_queues.begin(), port_queues.end());
    // 当前无端口能存放流，那么就更新时间
    while (true)
    {
        Flow flow;
        // cur_flows.end_flow().bw <= port_queues[0].cur_available_bandwidth &&
        if (!cur_flows.empty() && cur_flows.search_and_pop(port_queues[0].cur_available_bandwidth, flow)){
            results.push_back(Result{flow.id, port_queues[0].id, cur_time});
            port_queues[0].heap.push(FlowLog{flow, flow.nt+cur_time});
            port_queues[0].cur_available_bandwidth -= flow.bw; 
            make_heap(port_queues.begin(), port_queues.end()); // 维护大根堆
        }else{
            break;
        }   
    }
    // 更新时间
    int tem = INT_MAX;
    for (int i=0; i<port_queues.size(); i++)
        if (!port_queues[i].heap.empty() && tem > port_queues[i].heap.top().end_time)
            tem = port_queues[i].heap.top().end_time;
    cur_time = tem;
}

int main() {
    int file_num = 0;
    time_t t1;
    time(&t1);
    while (true){
        vector<Port> ports;
        vector<Flow> flows;
        vector<Result> results;
        debug_output.open("./test.txt", ios::out);
        // 1. 读取端口和流文件
        if (!read_data(ports, flows, file_num))
            break;
        sort(flows.begin(), flows.end(), cmp_by_et); // 按进入时间排序
        // 2. 得到端口队列类
        vector<PortQueue> port_queues;
        create_port_queues(ports, port_queues);
        cur_time = flows[0].et; // 得到起始时间
        cur_fi = 0; // 起始流索引

        // 3. 规划流
        while (cur_fi < flows.size() || !cur_flows.empty()){
            // if (!cur_flows.empty()){
            //     // print_port_cur_available_bw(port_queues);
            //     for (int i=0; i<port_queues.size(); i++)
            //         debug_output << port_queues[i].cur_available_bandwidth << " ";
            //     debug_output << '\n' <<endl;
            // }
            get_new_flows(flows);
            flow_assign(port_queues, results);
        }
        // 4. 将结果输出
        ofstream output;
        char result_path[50];
        sprintf(result_path, "%s%d/result.txt", ROOT, file_num);
        output.open(result_path, ios::out);
        for (int ri=0; ri<results.size(); ri++){
            if (ri == results.size()-1)
                output << results[ri].fid << ',' << results[ri].pid << ',' << results[ri].st;
            else
                output << results[ri].fid << ',' << results[ri].pid << ',' << results[ri].st << endl;
        }
        output.close();
        
        file_num += 1;
        vector<PortQueue>().swap(port_queues);
        vector<Port>().swap(ports);
        vector<Result>().swap(results);
        // break;

    }
    time_t t3;
    time(&t3);
    cout << difftime(t3, t1) <<endl;
    return 0;
}
