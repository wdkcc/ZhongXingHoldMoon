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

// 数据文件夹
const char ROOT[] = "../data/";
int port_time=0, cur_fi=0, buffer_max, cur_seed_time, next_time_flows_num, buffer_queue_max=30;
ofstream debug_output;
// 用来判断流是否还在缓存队列中的结构体
struct FlowEht{
    int id; // 流ID
    int t; // 流进入发送堆的时间
};
// 流的结构体
struct Flow{
    int id;
    int bw; // bandwidth
    int et; //enter time
    int nt; //发送need time
    bool operator < (const Flow &f) const{
        return this->bw < f.bw;
    }
};
// 端口的结构体
struct Port{
    int id;
    int bw;
};
// 存储结果的结构体
struct Result{
    int fid;
    int pid;
    int st; // seed_time
};
// 流记录，用来存储在端口的发送堆中，从而判断流是否已经被发送
struct FlowLog{
    Flow flow;
    int end_time; // 发送结束时间
    bool operator < (const FlowLog &f) const{
        return this->end_time > f.end_time;
    }
};
// 端口队列类
class PortQueue{
public:
    int id;
    int bw;
    priority_queue<FlowLog> heap; // 用来模拟流在端口发送的堆
    int cur_available_bandwidth; // 当前端口在当前时刻的可用带宽
    queue<FlowEht> buffer_q; // 模拟端口缓存区的队列

    PortQueue(int pid, int bandwidth){
        id = pid;
        bw = bandwidth;
        cur_available_bandwidth = bw;
    }
    bool operator < (const PortQueue &f) const{
        return this->cur_available_bandwidth < f.cur_available_bandwidth; // <是大根堆
    }
    // 将流记录放入端口堆中
    void put_port_heap(FlowLog flowlog){
        heap.push(flowlog);
        cur_available_bandwidth -= flowlog.flow.bw;
    }
    // 更新端口堆区的流数据
    void update_port_heap(){
        while (!heap.empty() && heap.top().end_time <= port_time){
            FlowLog flowlog = heap.top();
            heap.pop();
            cur_available_bandwidth += flowlog.flow.bw;
        }
    }
    // 将流放入端口堆中发送
    void push(Flow flow){
        put_port_heap(FlowLog{flow, flow.nt+port_time});
    }
    // 更新缓存队列
    void update_buffer_queue(){
        while (!buffer_q.empty() && buffer_q.front().t <= cur_seed_time)
            buffer_q.pop();
    }

};
// 存放正在发送流的结构体
struct FlowNode{
    Flow data;
    FlowNode* next;
    FlowNode* front;
};
// 自定义的按直接插入方式排序的队列
class DirectInsertQueue{
private:
    FlowNode* head;
    FlowNode* end;
    bool (*cmp)(Flow, Flow); // 排序比较的方法
public:
    int size;
    DirectInsertQueue(bool (*cmp_fuc)(Flow, Flow)){
        head = (FlowNode*)malloc(sizeof(FlowNode));
        head->next = NULL;
        head->front == NULL;
        end = head;
        cmp = cmp_fuc;
        size = 0;
    }
    // 取队顶元素
    Flow top(){
        return head->next->data;
    }
    // 入队
    void push(Flow f){
        FlowNode *node = (FlowNode*)malloc(sizeof(FlowNode));
        node->data = f;
        node->next = NULL;
        node->front = NULL;
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
        if (node->next != NULL)
            node->next->front = node;
        node->front = temhead;
        size += 1;
    }
    // 搜索满足能够容纳得了的流，找到了就通过引用传递返回该流，否则返回false
    bool search_and_pop(int available_bw, Flow &f){
        FlowNode* temhead = head;
        while (temhead->next != NULL){
            if (temhead->next->data.bw <= available_bw){
                if (temhead->next == end)
                    end = temhead;
                f = temhead->next->data;
                temhead->next = temhead->next->next;
                if (temhead->next != NULL)
                    temhead->next->front = temhead;
                size -= 1;
                return true;
            }else
                temhead = temhead->next;
        }
        return false;
    }
    // 判断队列是否为空
    bool empty(){
        if (head->next == NULL)
            return true;
        return false;
    }
    // 得到队尾的流
    Flow end_flow(){
        return end->data;
    }  

    // 删除最后一个结点
    // void pop_end(Flow& f, int bw){
    //     FlowNode* temend = end->front;
    //     end = temend;
    //     // 保存并删除该flow
    //     f = temend->next->data;
    //     // 删除
    //     temend->next = temend->next->next;
    //     size -= 1;
    // }


    // 删除第一个结点
    bool pop_front(Flow& f, int bw){
        // 保存
        f = head->next->data;
        // 删除
        if (f.bw<=bw){
            head->next = head->next->next;
            if (head->next != NULL)
                head->next->front = head;
            size -= 1;
            return true;
        }
        return false;
    }
};
// 放入直接插入队列的流的比较规则
bool cmp_by_bw_nt(Flow f1, Flow f2){
    float w1 = f1.bw - f1.nt*2.5;
    float w2 = f2.bw - f2.nt*2.5;
    return w1 > w2; // <是递增 >是递减
}

// 放入直接插入队列的流的比较规则
// bool cmp_by_bw_nt(Flow f1, Flow f2){
//     float w1 = float(f1.bw)/f1.nt;
//     float w2 = float(f2.bw)/f2.nt;
//     if (abs(w1-w2) >= 0.012)
//         return w1 > w2;
//     else
//         return f1.nt > f2.nt;
//     return w1 > w2; // <是递增 >是递减
// }

// 创建端口队列
void create_port_queues(vector<Port> ports, vector<PortQueue> &port_queues, PortQueue &pq_max_bw){
    pq_max_bw = PortQueue(ports[0].id, ports[0].bw);
    for (int i=0; i<ports.size(); i++){
        port_queues.push_back(PortQueue(ports[i].id, ports[i].bw));
        if (pq_max_bw.bw < ports[i].bw)
            pq_max_bw = port_queues[i];
    }
}
// 从文件中读取数据
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
// 按进入时间比较
int cmp_by_et(Flow f1, Flow f2){
    return f1.et < f2.et;
}

DirectInsertQueue dispatching_flows = DirectInsertQueue(cmp_by_bw_nt);
// 得到新进入设备的流
void get_new_flows(vector<Flow> flows, vector<PortQueue> port_queues){
    // 流已经遍历完，就停止遍历。为了防止缓存队列堵住导致无法发送流，则更新时间port_queues[0].buffer_q.front().t;
    if (cur_fi >= flows.size()){
        next_time_flows_num = 0;
        if (!port_queues[0].buffer_q.empty())
            cur_seed_time = port_queues[0].buffer_q.front().t;
        else
            cur_seed_time += 1;
        return;
    }
    // 更新cur_seed_time
    if (cur_seed_time < flows[cur_fi].et)
        cur_seed_time = flows[cur_fi].et;
    // 如果port_time太小，则更新port_time
    if (port_time < cur_seed_time)
        port_time = cur_seed_time;

    while (true){
        // 得到下一时刻进入设备的流数量
        int tem_cur_fi = cur_fi, tem_time = flows[cur_fi].et;
        next_time_flows_num = 0;
        for (;tem_cur_fi < flows.size() && flows[tem_cur_fi].et == tem_time; tem_cur_fi++)
            next_time_flows_num += 1;
        // 看下一刻流的数量加当前时刻的会不会超过调度区最大容量，不超过则接收下一时刻的流，负责退出对流进行分配
        if (dispatching_flows.size + next_time_flows_num <= buffer_max && tem_time <= port_time){
            cur_seed_time = flows[cur_fi].et;
            while(next_time_flows_num > 0){
                dispatching_flows.push(flows[cur_fi]);
                cur_fi += 1;
                next_time_flows_num -= 1;
            }
        }else{
            break;
        }
        if (tem_cur_fi >= flows.size())
            break;
    }  
}
// 更新端口堆区的时间port_time
void update_port_time(vector<PortQueue> &port_queues){
    int tem = INT_MAX;
    for (int i=0; i<port_queues.size(); i++)
        if (!port_queues[i].heap.empty() && tem > port_queues[i].heap.top().end_time)
            tem = port_queues[i].heap.top().end_time;
    port_time = tem;
}
// 判断全部的端口堆区是不是空的
bool is_free_port_heap(vector<PortQueue> &port_queues){
    for (int i=0; i<port_queues.size(); i++)
        if (!port_queues[i].heap.empty())
            return false;
    return true;
}
// 将流丢弃
void drop_flow(vector<Result> &results, PortQueue pq_max_bw){
    Flow f;
    dispatching_flows.pop_front(f, pq_max_bw.bw);
    results.push_back(Result{f.id, pq_max_bw.id, cur_seed_time});
}

// 输入当前时刻的流数据，端口队列。将当前时刻的流分配给对应端口
void flow_assign(vector<PortQueue> &port_queues, vector<Result> &results, PortQueue pq_max_bw){
    // 当前无端口能存放流，那么就更新时间
    do {
        // 更新当前端口带宽和端口缓存区队列
        for (int i=0; i<port_queues.size(); i++){
            port_queues[i].update_port_heap();
            port_queues[i].update_buffer_queue();
        }
        make_heap(port_queues.begin(), port_queues.end());
        bool is_port_queue_full = false;
        // 将流数据送入端口。若流数据过多导致端口带宽满，就将时间短的高带宽流丢弃。
        while (true){    
            // 检查要送入的端口缓冲区是否已经满了。满了则选择要丢弃的流，丢弃完成直接break;
            if (port_queues[0].buffer_q.size() >= buffer_queue_max){
                while (dispatching_flows.size+next_time_flows_num > buffer_max){
                    drop_flow(results, pq_max_bw);
                }
                break;
            }
            Flow flow;
            if (!dispatching_flows.empty()){
                // 为当前端口分配流。分配失败则有两种情况。一种是端口可用容量太小；一种是流带宽过大
                if (dispatching_flows.search_and_pop(port_queues[0].cur_available_bandwidth, flow)){
                    results.push_back(Result{flow.id, port_queues[0].id, cur_seed_time});
                    if (cur_seed_time < port_time)
                        port_queues[0].buffer_q.push(FlowEht{flow.id, port_time}); // 将流发送至对应端口中，记录该流进入端口堆的时间
                    port_queues[0].heap.push(FlowLog{flow, flow.nt+port_time});
                    port_queues[0].cur_available_bandwidth -= flow.bw; 
                    make_heap(port_queues.begin(), port_queues.end()); // 维护大根堆
                }else{
                    is_port_queue_full = true;
                    while (dispatching_flows.size+next_time_flows_num > buffer_max){
                        drop_flow(results, pq_max_bw);
                    }
                    break;
                }
            }else{
                break;
            }   
        }
        if (is_port_queue_full && !is_free_port_heap(port_queues))
            update_port_time(port_queues);
    }while (dispatching_flows.size+next_time_flows_num > buffer_max);

}

int main() {
    int file_num = 0;
    time_t t1;
    time(&t1);
    while (file_num < 10){
        vector<Port> ports;
        vector<Flow> flows;
        vector<Result> results;
        PortQueue pq_max_bw = PortQueue(-1, -1);
        // 1. 读取端口和流文件
        if (!read_data(ports, flows, file_num))
            break;
        buffer_max = ports.size() * 20; // 得到调度缓存区最大流容量
        sort(flows.begin(), flows.end(), cmp_by_et); // 按进入时间排序
        // 2. 得到端口队列类
        vector<PortQueue> port_queues;
        create_port_queues(ports, port_queues, pq_max_bw);
        // 2.1 得到端口的缓存队列
        port_time = flows[0].et; // 得到起始时间
        cur_seed_time = flows[0].et; // 得到起始发送时间
        cur_fi = 0; // 起始流索引
        // 3. 规划流
        while (cur_fi < flows.size() || !dispatching_flows.empty()){
            // 取出下一时刻的流放入流调度区
            get_new_flows(flows, port_queues);
            // 分配流，放入端口
            flow_assign(port_queues, results, pq_max_bw);
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
        // 5. 清空容器 
        vector<PortQueue>().swap(port_queues);
        vector<Port>().swap(ports);
        vector<Result>().swap(results);
        // break;
    }
    time_t t2;
    time(&t2);
    cout << difftime(t2, t1) <<endl;
    return 0;
}
