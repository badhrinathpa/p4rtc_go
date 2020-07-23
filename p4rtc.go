package p4rtc_bad

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/golang/protobuf/proto"
    p4_config_v1 "github.com/p4lang/p4runtime/go/p4/config/v1"
	p4 "github.com/p4lang/p4runtime/go/p4/v1"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc"
)

type P4DeviceConfig []byte

const invalidID = 0
const (
      FIELD_TYPE_EXACT      uint8=0
      FIELD_TYPE_LPM        uint8=1
      FIELD_TYPE_TERNARY    uint8=2
      FIELD_TYPE_RANGE      uint8=3
      FUNCTION_TYPE_INSERT  uint8=4
      FUNCTION_TYPE_UPDATE  uint8-5
      FUNCTION_TYPE_DELETE  uint8=6
)

type Intf_Table_Entry struct {
    Ip          uint32
    Prefix_Len  uint32
    Src_Intf    string
    Direction   string
}

type Action_Param struct {
	Len            uint32
	Name           string
	Value          []byte
}

type Match_Field struct {
	Type           Field_Type
	Len            uint32
	Prefix_Len     uint32
	Name           string
	Value          []byte
	mask           []byte
}

type AppTableEntry struct {
	Field_Size     uint32
	Param_Size     uint32
	Table_Name     string
	Action_Name    string
	Fields         []Match_Field
	Params         []Action_Param
}

type P4rtClient struct {
	Client         p4.P4RuntimeClient
	P4Info         p4_config_v1.P4Info
	Stream         p4.P4Runtime_StreamChannelClient
	DeviceID       uint64
	ElectionID     p4.Uint128
}


func (c *P4rtClient) tableId(name string) uint32 {
    if c.P4Info == nil {
        return invalidID
    }
    for _, table := range c.P4Info.Tables {
        if table.Preamble.Name == name {
            return table.Preamble.Id
        }
    }
    return invalidID
}

func (c *P4rtClient) addMatchField(matchField Match_Field) uint32 {
    if c.P4Info == nil {
        return invalidID
    }
    for _, table := range c.P4Info.Tables {
        if table.Preamble.Name == name {
            return table.Preamble.Id
        }
    }
    return invalidID
}

func (c *P4rtClient) actionId(name string) uint32 {
    if c.P4Info == nil {
        return invalidID
    }
    for _, action := range c.P4Info.Actions {
        if action.Preamble.Name == name {
            return action.Preamble.Id
        }
    }
    return invalidID
}

func (c *P4rtClient) SetMastership(electionID p4.Uint128) (err error) {
	c.ElectionID = electionID
	mastershipReq := &p4.StreamMessageRequest{
		Update: &p4.StreamMessageRequest_Arbitration{
			Arbitration: &p4.MasterArbitrationUpdate{
				DeviceId:   1,
				ElectionId: &electionID,
			},
		},
	}
	err = c.Stream.Send(mastershipReq)
	return
}

func (c *P4rtClient) Init() (err error) {
	// Initialize stream for mastership and packet I/O
	c.Stream, err = c.Client.StreamChannel(context.Background())
	if err != nil {
		fmt.Printf("stream channel error: %v\n", err)
		return
	}
	go func() {
		for {
			res, err := c.Stream.Recv()
			if err != nil {
				fmt.Printf("stream recv error: %v\n", err)
			} else if arb := res.GetArbitration(); arb != nil {
				if code.Code(arb.Status.Code) == code.Code_OK {
					fmt.Println("client is master")
				} else {
					fmt.Println("client is not master")
				}
			} else {
				fmt.Printf("stream recv: %v\n", res)
			}

		}
	}()

	fmt.Println("exited from recv thread.")
	return
}

type ExactMatch struct {
    Value []byte
}

func (m *ExactMatch) get(ID uint32) *p4_v1.FieldMatch {
    exact := &p4_v1.FieldMatch_Exact{
        Value: m.Value,
    }
    mf := &p4_v1.FieldMatch{
        FieldId:        ID,
        FieldMatchType: &p4_v1.FieldMatch_Exact_{exact},
    }
    return mf
}

type LpmMatch struct {
    Value []byte
    PLen  int32
}

func (m *LpmMatch) get(ID uint32) *p4_v1.FieldMatch {
    lpm := &p4_v1.FieldMatch_LPM{
        Value:     m.Value,
        PrefixLen: m.PLen,
    }

    // P4Runtime now has strict rules regarding ternary matches: in the
    // case of LPM, trailing bits in the value (after prefix) must be set
    // to 0.
    firstByteMasked := int(m.PLen / 8)
    if firstByteMasked != len(m.Value) {
        i := firstByteMasked
        r := m.PLen % 8
        m.Value[i] = m.Value[i] & (0xff << (8 - r))
        for i = i + 1; i < len(m.Value); i++ {
            m.Value[i] = 0
        }
    }

    mf := &p4_v1.FieldMatch{
        FieldId:        ID,
        FieldMatchType: &p4_v1.FieldMatch_Lpm{lpm},
    }
    return mf
}

func (c *P4rtClient) InsertTableEntry(tableEntry AppTableEntry, table string, action string, mfs []MatchInterface, params [][]byte) error {

    fmt.Printf("Insert Table Entry for Table %s\n", tableEntry.Table_Name)
    tableID := c.tableId(tableEntry.Table_Name)
    actionID := c.actionId(tableEntry.Action_Name)
    fmt.Printf("adding fields\n");
    for mf := range tableEntry.fields {
        addFieldValue(tableEntry, tb_name, mf);
    }
    directAction := &p4_v1.Action{
        ActionId: actionID,
    }

    for idx, p := range params {
        param := &p4_v1.Action_Param{
            ParamId: uint32(idx + 1),
            Value:   p,
        }
        directAction.Params = append(directAction.Params, param)
    }

    tableAction := &p4_v1.TableAction{
        Type: &p4_v1.TableAction_Action{directAction},
    }

    entry := &p4_v1.TableEntry{
        TableId:         tableID,
        Action:          tableAction,
        IsDefaultAction: (mfs == nil),
    }

    for idx, mf := range mfs {
        entry.Match = append(entry.Match, mf.get(uint32(idx+1)))
    }

    var updateType p4_v1.Update_Type
    if mfs == nil {
        updateType = p4_v1.Update_MODIFY
    } else {
        updateType = p4_v1.Update_INSERT
    }
    update := &p4_v1.Update{
        Type: updateType,
        Entity: &p4_v1.Entity{
            Entity: &p4_v1.Entity_TableEntry{entry},
        },
    }

    return c.WriteUpdate(update)
}

func (c *P4rtClient) SetForwardingPipelineConfig(p4InfoPath, deviceConfigPath string) (err error) {
	fmt.Printf("P4 Info: %s\n", p4InfoPath)

	p4infoBytes, err := ioutil.ReadFile(p4InfoPath)
	if err != nil {
		fmt.Printf("Read p4info file error %v\n", err)
		return
	}
	
	var p4info p4_config_v1.P4Info
	err = proto.UnmarshalText(string(p4infoBytes), &p4info)
	if err != nil {
		fmt.Printf("Unmarshal test failed for p4info %v", err)
		return
	}

	c.P4Info = p4info 
	deviceConfig, err := LoadDeviceConfig(deviceConfigPath)
	if err != nil {
		fmt.Printf("bmv2 json read failed %v",err)
		return 
	}
	
	var pipeline p4.ForwardingPipelineConfig
	pipeline.P4Info = &p4info
	pipeline.P4DeviceConfig = deviceConfig
	
	err = SetPipelineConfig(c.Client, c.DeviceID, &c.ElectionID, &pipeline)
	if err != nil {
		fmt.Printf("set pipeline config error %v",err)
		return
	}
	return
}

func SetPipelineConfig(client p4.P4RuntimeClient, deviceID uint64, electionID *p4.Uint128, config *p4.ForwardingPipelineConfig) error {
    req := &p4.SetForwardingPipelineConfigRequest{
        DeviceId: deviceID,
        RoleId:   0,
        ElectionId: electionID,
        Action: p4.SetForwardingPipelineConfigRequest_VERIFY_AND_COMMIT,
        Config: config,
    }
	_, err := client.SetForwardingPipelineConfig(context.Background(), req)
	if err != nil {
		fmt.Printf("set forwarding pipeline returned error %v", err)
	}
	return err
}

func GetConnection(host string) (conn *grpc.ClientConn, err error) {
	/* get connection */
	log.Println("Get connection.")
	conn, err = grpc.Dial(host, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("grpc dial err: %v\n", err)
		return nil, err
	}
	return
}

// LoadDeviceConfig : Load Device config
func LoadDeviceConfig(deviceConfigPath string) (P4DeviceConfig, error) {
	fmt.Printf("BMv2 JSON: %s\n", deviceConfigPath)

	deviceConfig, err := os.Open(deviceConfigPath)
	if err != nil {
		return nil, fmt.Errorf("open %s: %v", deviceConfigPath, err)
	}
	defer deviceConfig.Close()
	bmv2Info, err := deviceConfig.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat %s: %v", deviceConfigPath, err)
	}

	bin := make([]byte, int(bmv2Info.Size()))
	if b, err := deviceConfig.Read(bin); err != nil {
		return nil, fmt.Errorf("read %s: %v", deviceConfigPath, err)
	} else if b != int(bmv2Info.Size()) {
		return nil, errors.New("bmv2 bin copy failed")
	}

	return bin, nil
}

func CreateChannel(host string, deviceID uint64) (*P4rtClient, error) {
	log.Println("create channel")
	// Second, check to see if we can reuse the gRPC connection for a new P4RT client
	conn, err := GetConnection(host)
	if err != nil {
		log.Println("grpc connection failed")
		return nil, err
	}

	client := &P4rtClient{
		Client:   p4.NewP4RuntimeClient(conn),
		DeviceID: deviceID,
	}

	err = client.Init()
	if err != nil {
		fmt.Printf("Client Init error: %v\n", err)
		return nil, err
	}

	err = client.SetMastership(p4.Uint128{High: 0, Low: 1})
	if err != nil {
		fmt.Printf("Set Mastership error: %v\n", err)
		return nil, err
	}

	return client, nil
}
