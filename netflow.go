package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"sync"
	"strings"
	"time"
	"encoding/binary"
	"bytes"
	"log"

	"github.com/tehmaze/netflow/netflow9"
	"github.com/tehmaze/netflow/session"
	"github.com/tehmaze/netflow"
	"encoding/json"
	"strconv"
)

var (
	profiling            = flag.Bool("profiling", false, "Print profiling information")
	netflowAddress       = flag.String("netflow.listen-address", ":2055", "Network address on which to accept netflow binary network packets.")
	listenAddress        = flag.String("web.listen-address", ":9191", "Address on which to expose metrics.")
	tcpOutputAddr        = flag.String("tcp.output", "localhost:9200", "Address to send JSON data to.")
	numBatches           = flag.Int("ringBuffer.num-batches", 20, "Number of batches to allocate in memory.")
	bufferSize           = flag.Int("ringBuffer.udp-size", 16*2014, "Buffer size for each UDP packet.")
	inputBatchSize       = flag.Int("ringBuffer.input-batch-size", 200, "Size of the batch the dispatcher sends to the processing threads.")
	outputBatchSize      = flag.Int("ringBuffer.output-batch-size", 5000, "Size of the batch the output threads send to logstash.")
	numProcessingThreads = flag.Int("threads.processing", 4, "Number of processing threads.")
	numOutputThreads     = flag.Int("threads.output", 4, "Number of output threads.")

	ethTranslate = make(map[int64]string)
)

// Basic dict
type dict map[string]string

// Stats counter
type stat struct {
	counter string
	value   int64
}

type netflowMessage map[string]interface{}

// netflowPacket stores the UDP packet content and the size that was read
type netflowPacket struct {
	buffer []byte
	len    int
}

// netflowCollector is our main class
type netflowCollector struct {
	ringMu     *sync.Mutex         // Mutex to access the ringBuffer buffer
	ringIn     int                 // Point of insertion in the ringBuffer buffer
	ringOut    int                 // Point of removal in the ringBuffer buffer
	ringBuffer [][]*netflowPacket  // The ringBuffer buffer
	chStats    chan stat           // Channel to send data to the statistics thread
	stats      string              // Global statistics
	cond       *sync.Cond          // Cond used to dispatch batches to the processing threads via wait/signal
	chOut      chan netflowMessage // Channel used to dispatch messages to the output threads
}

// newNetFlowCollector initializes our main class
func newNetflowCollector() *netflowCollector {
	c := &netflowCollector{
		ringIn:     0,
		ringOut:    0,
		ringMu:     &sync.Mutex{},
		ringBuffer: make([][]*netflowPacket, *numBatches),
		chStats:    make(chan stat, 20),
		cond:       sync.NewCond(&sync.Mutex{}),
		chOut:      make(chan netflowMessage, 5),
	}

	for i := 0; i < *numProcessingThreads; i++ {
		go c.processingThread(fmt.Sprintf("processing thread %d", i))
	}

	for i := 0; i < *numOutputThreads; i++ {
		go c.outputThread(fmt.Sprintf("output thread %d", i))
	}

	for i := 0; i < *numBatches; i++ {
		c.ringBuffer[i] = makeBatch()
	}

	go c.statsThread()

	return c
}

// statsThread is the thread that collects statistics
func (c *netflowCollector) statsThread() {
	interval := 5
	stats := make(map[string]int64)
	stats5 := make(map[string]int64)
	ticker := time.NewTicker(time.Second * time.Duration(interval))
	for {
		select {
		case in := <-c.chStats:
			if _, ok := stats[in.counter]; ok {
				stats[in.counter] += in.value
			} else {
				stats[in.counter] = in.value
			}
		case <-ticker.C:
			s := make(map[string]int64)
			for k, v := range stats {
				if v1, ok := stats5[k]; ok {
					s[k + "/s"] = (v - v1)/5
				} else {
					s[k + "/s"] = v/5
				}
				stats5[k] = v
			}

			bytes1, err1 := json.MarshalIndent(stats, "", "  ")
			bytes2, err2 := json.MarshalIndent(s, "", "  ")
			if err1 != nil || err2 != nil {
				log.Printf("Error converting statistics to string: %s\n", err1.Error()+err2.Error())
				continue
			}
			c.stats = "Totals:\n" + string(bytes1) + "\nLast 5 seconds:\n" + string(bytes2)
			if *profiling {
				log.Println(c.stats)
			}

		}
	}
}

// outputThread is responsible for receiving netflowMessage objects via channel,
// encoding them in JSON in batches defined by outputBatchSize and sending the
// result through TCP to tcpOutputAddr.
func (c *netflowCollector) outputThread(threadName string) {
	writer := bytes.NewBuffer(make([]byte, 0, int(1.2*300 * *outputBatchSize)))
	encoder := json.NewEncoder(writer)
	counter := 0
	for {
		msg := <-c.chOut
		encoder.Encode(msg)
		counter = (counter + 1) % *outputBatchSize
		if counter == 0 {
			conn, err := net.Dial("tcp", *tcpOutputAddr)
			if err == nil {
				n, err := conn.Write(writer.Bytes())
				if err != nil {
					log.Printf("Error in %s writing to tcp socket: %s", threadName, err.Error())
					conn.Close()
					c.chStats <- stat{"output errors", 1}
					continue
				}
				c.chStats <- stat{"output bytes", int64(n)}
				conn.Close()
			} else {
				c.chStats <- stat{"output connection error", 1}
				log.Println(err)
			}
			writer.Reset()
		}
	}
}

// processingThread is a thread that waits for a batch to be sent by the dispatcher thread.
// When it arrives, it decodes all the packets in the batch to netflow9 messages.
func (c *netflowCollector) processingThread(threadName string) {
	// localBatch is allocated so it can be switched with the batches in the ringBuffer buffer,
	// preventing any buffer allocation during processing
	localBatch := makeBatch()

	s := session.New()
	d := netflow.NewDecoder(s)

	// Let's wait things get ready
	time.Sleep(time.Second)

	for {
		// Waits for a batch to be ready
		c.cond.L.Lock()
		c.cond.Wait()
		c.cond.L.Unlock()

		// Swtiches localBatch with the next batch that is ready in the ringBuffer buffer
		f := func() bool {
			defer c.ringMu.Unlock()
			c.ringMu.Lock()
			if c.ringOut != c.ringIn {
				temp := localBatch
				localBatch = c.ringBuffer[c.ringOut]
				c.ringBuffer[c.ringOut] = temp
				c.ringOut = (c.ringOut + 1) % *numBatches
				return true
			} else {
				return false
			}
		}

		if !f() {
			log.Printf("%s woken, but no data to read", threadName)
			c.chStats <- stat{"empty wake", 1}
			continue
		}

		// Checks if there was any packet loss
		seq1 := uint32(binary.BigEndian.Uint32(localBatch[0].buffer[12:16]))
		seq2 := uint32(binary.BigEndian.Uint32(localBatch[*inputBatchSize-1].buffer[12:16]))
		if int(seq2-seq1) != *inputBatchSize-1 {
			log.Printf("Lost packet in %s %d %d %d\n", threadName, seq1, seq2, seq2-seq1)
			c.chStats <- stat{"wrong packet numbering", 1}
		}

		// Processes packets
		for i := 0; i < *inputBatchSize; i++ {
			m, err := d.Read(bytes.NewBuffer(localBatch[i].buffer[:localBatch[i].len]))

			// TODO This happens sometimes, when it shouldn't. WHY?
			if err != nil {
				log.Printf("Reading error from UDP packet, EOF? Error = %s, entry number = %d, packet length = %d", err.Error(), i, localBatch[i].len)
				continue
			}
			p, ok := m.(*netflow9.Packet)
			if !ok {
				log.Printf("Unable to decode netflow9 packet")
				c.chStats <- stat{"decoding error", 1}
				continue
			}
			c.processNetflow9Packet(p)
		}
	}
}

// processNetflow9Packet receives a netflow 9 packet and decodes it into a dictionary.
// After doing that, it sends the result to the output threads via a channel.
func (c *netflowCollector) processNetflow9Packet(p *netflow9.Packet) {

	totalFlows := int64(0)
	totalFlowSets := int64(0)

	// Loop that processes all flowSets
	for _, set := range p.DataFlowSets {
		totalFlowSets++

		// Loop that processes all flows
		for _, record := range set.Records {
			totalFlows++
			fields := make(netflowMessage)

			// Loop that processes all fields in a flow
			for _, field := range record.Fields {
				if field.Type == 56701 { // TODO: PALO ALTO HACKING
					fields["appid"] = stringFromByteZero(field)
				} else if field.Type == 56702 { // TODO: PALO ALTO HACKING
					fields["userid"] = stringFromByteZero(field)
				} else {
					fields[field.Translated.Name] = field.Translated.Value
				}
			}

			if len(fields) > 0 {

				// TODO: Implement in logstash
				inIf := fields["ingressInterface"].(uint32)
				fields["ingressInterface"] = ethTranslate[int64(inIf)]
				outIf := fields["egressInterface"].(uint32)
				fields["egressInterface"] = ethTranslate[int64(outIf)]

				fields["@timestamp"] = time.Now().Format(time.RFC3339)
				c.chOut <- fields
			} else {
				c.chStats <- stat{"empty flows", 1}
			}
		}
	}
	c.chStats <- stat{"total flows", totalFlows}
	c.chStats <- stat{"total flowsets", totalFlowSets}
}

// dispatcherThread is the main thread. It reads UDP packets from the socket,
// groups them in batches, adds the batches to a ringBuffer buffer then
// dispatches them to the processingThread thread via signal.
func (c *netflowCollector) dispatcherThread(udpSock *net.UDPConn) {
	defer udpSock.Close()
	time.Sleep(time.Second * 2)

	// Swapped with the batch in the ringBuffer buffer
	localBatch := makeBatch()

	for {
		// Fetches a full UDP packet batch
		for i := 0; i < *inputBatchSize; i++ {
			l, srcAddress, err := udpSock.ReadFromUDP(localBatch[i].buffer)
			localBatch[i].len = l
			if err != nil {
				log.Printf("Error reading UDP packet from %s: %s", srcAddress, err)
				c.chStats <- stat{"udp reading errors", 1}
				continue
			}
		}

		f := func() {
			defer c.ringMu.Unlock()
			// Libera um batch para as threads consumidoras
			c.ringMu.Lock()

			if (c.ringIn+1)%*numBatches == c.ringOut {
				log.Println("Batch lost...")
				c.chStats <- stat{"lost batches", 1}
				return
			}
			temp := c.ringBuffer[c.ringIn]
			c.ringBuffer[c.ringIn] = localBatch
			localBatch = temp
			c.ringIn = (c.ringIn + 1) % *numBatches
		}

		f()
		c.cond.Signal()
		c.chStats <- stat{"batches sent to processing", 1}
	}
}

/////////////////////////////////////////////// GENERAL FUNCTIONS /////////////////////////////////////

// makeBatch allocates a batch of netflowPackets
func makeBatch() []*netflowPacket {
	batch := make([]*netflowPacket, *inputBatchSize)
	for i := 0; i < *inputBatchSize; i++ {
		batch[i] = &netflowPacket{
			buffer: make([]byte, *bufferSize),
			len:    0,
		}
	}
	return batch
}

// TODO migrate this code to logstash
// snmpToEth converts a palo alto snmp string representing an interface to a human readable format
func snmpToEth(i string) string {
	if len(i) != 9 || i[:3] != "101" {
		return i
	}

	phy := strings.TrimLeft(i[3:5], "0")
	vir := strings.TrimLeft(i[5:9], "0")

	if vir == "" {
		return fmt.Sprintf("eth%s", phy)
	} else {
		return fmt.Sprintf("eth%s.%s", phy, vir)
	}
}

// TODO Probably there is a simpler way
// stringFromByteZero fetches a string from a netflow9.Field
func stringFromByteZero(field netflow9.Field) string {
	var i uint16
	for i = 0; i < field.Length; i++ {
		if field.Bytes[i] == 0 {
			break
		}
	}

	return string(field.Bytes[0:i])
}

/////////////////////////////////////////////// END GENERAL FUNCTIONS /////////////////////////////////////

/////////////////////////////////////////////// MAIN /////////////////////////////////////
func main() {

	flag.Parse()

	// TODO migrate to logstash
	// Generates all eth interfaces for palo alto
	var strSNMP string
	var snmp int64
	for i := 1; i <= 20; i++ {
		for j := 0; j <= 9999; j++ {
			strSNMP = fmt.Sprintf("101%02d%04d", i, j)
			snmp, _ = strconv.ParseInt(strSNMP, 10, 64)
			ethTranslate[snmp] = snmpToEth(strSNMP)
		}
	}

	c := newNetflowCollector()

	udpAddress, err := net.ResolveUDPAddr("udp", *netflowAddress)
	if err != nil {
		log.Fatalf("Error resolving UDP address: %s", err)
	}
	udpSock, err := net.ListenUDP("udp", udpAddress)
	if err != nil {
		log.Fatalf("Error lisening to UDP address: %s", err)
	}
	go c.dispatcherThread(udpSock)

	// TODO show meaningful information about the exporter for monitoring
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
        <head><title>netflow Exporter</title></head>
        <body>
        <h1>netflow Exporter</h1>
        <pre>` + c.stats + `</pre>
        </body>
        </html>`))
	})

	log.Println("Listening on", *listenAddress)
	log.Println("Listening UDP on", *netflowAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
