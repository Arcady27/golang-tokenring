package main

import (
    "fmt"
    "net"
    "flag"
    "time"
    "encoding/json"
    "math/rand"
)

type Token struct {
    Type string //"notification" or "message"
    Sender int
    Origin int
    Dest int
    Data string
}

type ServiceMessage struct {
    Type string  //"send" or "drop"
    Dest  int
    Data string
}


func marshal_token(type_ string, sender int, origin int, dest int, data string) []byte {
    text := Token{
        Type: type_,
        Sender:   sender,
        Origin:   origin,
        Dest:    dest,
        Data:   data,
    }
    b, err := json.Marshal(text)
    if err != nil {
        fmt.Println("error:", err)
    }
    return b
}

func marshal_service_message(type_ string, dest int, data string) []byte {
    text := ServiceMessage{
        Type:   type_,
        Dest:    dest,
        Data:   data,
    }
    b, err := json.Marshal(text)
    if err != nil {
        fmt.Println("error:", err)
    }
    return b
}

func parse_token_json (json_string []byte) Token {
    c := make(map[string]interface{})
    e := json.Unmarshal(json_string, &c)
    CheckError(e)

    msg_type := c["Type"].(string)
    origin := int(c["Origin"].(float64))
    sender := int(c["Sender"].(float64))
    dest := int(c["Dest"].(float64))
    data := c["Data"].(string)
    
    return Token{msg_type, sender, origin, dest, data}
}

func parse_service_json (json_string []byte) ServiceMessage {
    c := make(map[string]interface{})
    e := json.Unmarshal(json_string, &c)
    CheckError(e)

    msg_type := c["Type"].(string)
    dest := int(c["Dest"].(float64))
    data := c["Data"].(string)
    
    return ServiceMessage{msg_type, dest, data}
}

func CheckError(err error) bool{
    if err != nil && !err.(net.Error).Timeout() {
        fmt.Println("Error: " , err)
        return false;
    } 
    return true;
}

func getServicePort(node int) int  {
    return 40000 + node;
}

func getMainPort(node int) int  {
    return 30000 + node;
}

func getNeighbor(n_nodes int, node int) int {
    return (node + 1) % n_nodes
}

func node_service_daemon(n_nodes int, node int, messages_queue *[]ServiceMessage, DropToken *bool) {
    
    //resolve service address
    ServiceServerAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", getServicePort(node)))
    CheckError(err)
    
    //listen to service port
    ServiceConn, err := net.ListenUDP("udp", ServiceServerAddr)
    CheckError(err)
    defer ServiceConn.Close()

    buf := make([]byte, 1024)
    for i:=0;;i+=1 {

        //read from service with timeout
        ServiceConn.SetReadDeadline(time.Now().Add(time.Second / 10))
        n, _, err := ServiceConn.ReadFromUDP(buf)
        CheckError(err)
        
        if err == nil {
            fmt.Println(fmt.Sprintf("node %d : received service message: %s", node, string(buf[0:n])))

            message := parse_service_json(buf[0:n])

            if message.Type == "send" { //add new message to queue
                *messages_queue = append(*messages_queue, message)
                fmt.Println(fmt.Sprintf("node %d : queue ", node), messages_queue)
            } else if message.Type == "drop" { //remember that needs to drop
                *DropToken = true
            }
        }
    }
}

func node_main_daemon(n_nodes int, node int, t int, StartTokenRing bool) {

    MainServerAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", getMainPort(node)))
    CheckError(err)
    //LocalAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:0"))
    //CheckError(err)

    MainConn, err := net.ListenUDP("udp", MainServerAddr)
    CheckError(err)
    defer MainConn.Close()

    NeighborAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", getMainPort(getNeighbor(n_nodes, node))))
    CheckError(err)
                
    buf := make([]byte, 1024)
    Waiting := false
    DropToken := false
    LastSentToken := Token{"",node, node, getNeighbor(n_nodes,node), ""}
    LastSentTokenTime := time.Now()
    messages_queue := make([]ServiceMessage,0)

    go node_service_daemon(n_nodes, node, &messages_queue, &DropToken)

    for i:=0;;i+=1 {
        MainConn.SetReadDeadline(time.Now().Add(time.Second))
        n, _, err := MainConn.ReadFromUDP(buf)
        CheckError(err)
        
        if err != nil { //timeout expired
            fmt.Println(fmt.Sprintf("node %d : timeout expired", node))
            if (Waiting == true && time.Since(LastSentTokenTime) > time.Second) || StartTokenRing == true { //if node sent and is waiting too long or it is first then create new token and send again
                fmt.Println(fmt.Sprintf("node %d : created new token", node))
                token_to_send := LastSentToken
                
                buf := marshal_token(token_to_send.Type, token_to_send.Sender, token_to_send.Origin, token_to_send.Dest, token_to_send.Data) 
                MainConn.WriteToUDP(buf, NeighborAddr)
                fmt.Println(fmt.Sprintf("node %d : send new token to %d", node, token_to_send.Dest))
                
                Waiting = true
                StartTokenRing = false
                LastSentToken = token_to_send
                LastSentTokenTime = time.Now()
            }

        } else { //got a message

            Waiting = false
            if DropToken == true { // drop, change state of DropToken and sleep for a while
                DropToken = false
                time.Sleep(time.Second * time.Duration(t) / 1000)
                continue
            }

            //parse token and keep for a while
            token := parse_token_json(buf[0:n])
            time.Sleep(time.Second * time.Duration(t) / 1000)
            token_to_send := Token {}
            
            //token is free
            if token.Data == "" {
                
                fmt.Println(fmt.Sprintf("node %d : received empty token from node %d, sending token to node %d", node, token.Sender, getNeighbor(n_nodes, node)))
                        
                //if something in the queue, use token to send
                if len(messages_queue) > 0 {
                    message_to_send := messages_queue[0]
                    messages_queue = messages_queue[1:]
                    token_to_send = Token{"message", node, node, message_to_send.Dest, message_to_send.Data}
                } else { //if nothing to send - just pass to next
                    token_to_send = token
                    token_to_send.Dest = getNeighbor(n_nodes, node)
                }
            } else  //token is full
            {   
                //token destination is current node
                if token.Dest == node { 

                    //if notification then free token
                    if token.Type == "notification" {
                
                        fmt.Println(fmt.Sprintf("node %d : received token from node %d with delivery confirmation from node %d, sending token to node %d", node, token.Sender, token.Origin, getNeighbor(n_nodes, node)))
                        token_to_send = Token{"", node, node, -1, ""}
                
                    } else { //if message then get data, send notification reply to token origin
                    
                        fmt.Println(fmt.Sprintf("node %d : received token from node %d with data from node %d (data=%s), sending token to node %d", node, token.Sender, token.Origin, token.Data, getNeighbor(n_nodes, node)))
                        token_to_send = Token{"notification", node, node, token.Origin, "reply"}
                    
                    }
                } else { //if token needs to go further then send to next
                    
                    fmt.Println(fmt.Sprintf("node %d : received token from node %d, sending token to node %d", node, token.Sender, getNeighbor(n_nodes, node)))
                    token_to_send = Token{token.Type, node, token.Origin, token.Dest, token.Data}
                
                }
            }
            
            //send token to next 
            buf := marshal_token(token_to_send.Type, token_to_send.Sender, token_to_send.Origin, token_to_send.Dest, token_to_send.Data) 
            MainConn.WriteToUDP(buf, NeighborAddr)
            //fmt.Println(fmt.Sprintf("node %d : send token %s to %d", node, string(buf), token_to_send.Dest))

            Waiting = true
            LastSentToken = token_to_send
            LastSentTokenTime = time.Now()
        }
    }
}

func send_service_message(dest int, msgtype string, param int) {
    LocalAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:0"))
    CheckError(err)
    RemoteAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", 40000 + dest))
    CheckError(err)
    Conn, err := net.DialUDP("udp", LocalAddr, RemoteAddr)
    
    buf := marshal_service_message(msgtype, dest, fmt.Sprintf("data_%d", param)) 
    Conn.Write(buf)
    Conn.Close()
}

func main() {
    rand.Seed(1)
    n_flag := flag.Int("n", 3, "nodes")
    t_flag := flag.Int("t", 2, "keep token milliseconds")
    flag.Parse()
    n_nodes := *n_flag
    time_interval := *t_flag
    fmt.Println(fmt.Sprintf("N=%d, T=%d", n_nodes, time_interval))

    for i := 0; i < n_nodes; i++ {
        if i == 0 {
            go node_main_daemon(n_nodes, i, time_interval, true)
        } else {
            go node_main_daemon(n_nodes, i, time_interval, false)
        }
    }
    
    //test tokenring
    for j:=1;j<20;j+=1 {
        time.Sleep(time.Second)
        if j % 5 != 0 {
            send_service_message(j % n_nodes, "send", j)
        } else {
            send_service_message(j % n_nodes, "drop", j)
        }

    }

    time.Sleep(time.Second*1000)
}