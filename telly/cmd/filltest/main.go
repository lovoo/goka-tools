package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lovoo/goka-tools/telly"
	"gopkg.in/rethinkdb/rethinkdb-go.v6"
)

type Report struct {
	ReportId string    `protobuf:"bytes,1,opt,name=report_id,json=reportId,proto3" json:"report_id,omitempty"`
	Created  time.Time `protobuf:"bytes,2,opt,name=created,proto3" json:"created,omitempty"`
	// Who created the report
	SenderId string `protobuf:"bytes,3,opt,name=sender_id,json=senderId,proto3" json:"sender_id,omitempty"`
	// Who was reported
	ReceiverId string `protobuf:"bytes,4,opt,name=receiver_id,json=receiverId,proto3" json:"receiver_id,omitempty"`
	// Report type
	Type string `protobuf:"bytes,5,opt,name=type,proto3" json:"type,omitempty"`
	// Text include in the report
	Text   string `protobuf:"bytes,6,opt,name=text,proto3" json:"text,omitempty"`
	Reason string `protobuf:"bytes,7,opt,name=reason,proto3" json:"reason,omitempty"`
	// Report origin
	Origin  string `protobuf:"varint,9,opt,name=origin,proto3,enum=lovoo.user.v1.ReportOrigin" json:"origin,omitempty"`
	IsValid bool   `protobuf:"varint,10,opt,name=is_valid,json=isValid,proto3" json:"is_valid,omitempty"`
	// Specifies whether the report comes from a user whose environment is
	// set to api_admin
	AdminReport bool   `protobuf:"varint,11,opt,name=admin_report,json=adminReport,proto3" json:"admin_report,omitempty"`
	AdjustId    string `protobuf:"bytes,12,opt,name=adjust_id,json=adjustId,proto3" json:"adjust_id,omitempty"`
	Ip          string `protobuf:"bytes,13,opt,name=ip,proto3" json:"ip,omitempty"`
}

type jsoncodec int

func (jc *jsoncodec) Encode(value interface{}) (data []byte, err error) {
	return json.Marshal(value)
}
func (jc *jsoncodec) Decode(data []byte) (value interface{}, err error) {
	var r Report
	return &r, json.Unmarshal(data, &r)
}

func main() {

	session, err := rethinkdb.Connect(rethinkdb.ConnectOpts{
		Address: "localhost",
	})
	foe("error connecting: %v", err)
	tele, err := telly.NewTelly(context.Background(), session, "reports", "reports", "reports", new(jsoncodec),
		telly.WithInitialLoad(10*time.Second),
		telly.WithRetention(30000*time.Hour, "something"),
		telly.WithInsertHook(func(key []byte, value interface{}) interface{} {
			report, ok := value.(*Report)
			if !ok {
				return nil
			}
			if strings.Contains(report.Text, "AdminTask.Evaluation") {
				return nil
			}

			return report
		}),
	)
	foe("error creating telly: %v", err)

	ctx, cancel := context.WithCancel(context.Background())

	waiter := make(chan os.Signal, 1)
	signal.Notify(waiter, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan struct{})
	go func() {
		defer close(done)
		foe("error running tele: %v", tele.Run(ctx, []string{"localhost:9092"}))
	}()

	<-waiter
	cancel()
	<-done

}

func foe(msg string, err error) {
	if err != nil {
		log.Fatalf(msg, err)
	}
}
