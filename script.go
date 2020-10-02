package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/jackc/pgx/v4/stdlib"
	"gitlab.com/thorchain/midgard/chain"
	"gitlab.com/thorchain/midgard/chain/notinchain"
	"gitlab.com/thorchain/midgard/internal/api"
	"gitlab.com/thorchain/midgard/internal/timeseries"
	"gitlab.com/thorchain/midgard/internal/timeseries/stat"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)
type Duration time.Duration

func (d Duration) WithDefault(def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	return time.Duration(d)
}


func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case string:
		v, err := time.ParseDuration(value)
		if err != nil {
			return err
		}
		*d = Duration(v)
	default:
		return errors.New("duration not a string")
	}
	return nil
}



type Config struct {
	ListenPort      int      `json:"listen_port"`
	ShutdownTimeout Duration `json:"shutdown_timeout"`
	ReadTimeout     Duration `json:"read_timeout"`
	WriteTimeout    Duration `json:"write_timeout"`

	TimeScale struct {
		Host     string `json:"host"`
		Port     int    `json:"port"`
		UserName string `json:"user_name"`
		Password string `json:"password"`
		Database string `json:"database"`
		Sslmode  string `json:"sslmode"`
	} `json:"timescale"`

	ThorChain struct {
		URL              string   `json:"url"`
		NodeURL          string   `json:"node_url"`
		ReadTimeout      Duration `json:"read_timeout"`
		LastChainBackoff Duration `json:"last_chain_backoff"`
	} `json:"thorchain"`
}

type poolData struct {
	BalanceRune  string	 `json:"balance_rune"`
	BalanceAsset string 	 `json:"balance_asset"`
	Asset        string  `json:"asset"`
	PoolUnits    string	 `json:"pool_units"`
	Status       string	 `json:"status"`
}

func main(){
	// read configuration
	var c Config
	switch len(os.Args) {
	case 1:
		break // refer to defaults
	case 2:
		c = *MustLoadConfigFile(os.Args[1])
	default:
		log.Fatal("one optional configuration file argument only—no flags")
	}
	SetupDatabase(&c)
	blocks := SetupBlockchain(&c)
	log.Print(blocks)

	file, err := os.Create("result.csv")
	checkError("Cannot create file", err)
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	lastBlockHeight, _, _, _ := timeseries.Setup()

	log.Print(int(lastBlockHeight))

	for offset := 67130; offset <= int(lastBlockHeight); offset++ {
		log.Print(offset)
		pool := "BNB.BNB"
		s := strconv.Itoa(offset)
		blockTimeStamp, err := timeseries.FetchTimestamp(s)
		if err != nil{
			log.Print(err)
		}
		log.Print(blockTimeStamp)
		BalanceRune, _, _, _, _ := CallAPI(s)
		TotalRuneStakes, _ := timeseries.GetTotalRuneStakes(pool,blockTimeStamp)
		TotalRunUnstakes, _ := timeseries.GetTotalRunUnstakes(pool,blockTimeStamp)
		TotalRuneSwapIn, _ := timeseries.GetTotalRuneSwapIn(pool, blockTimeStamp)
		TotalRuneSwapOut, _ := timeseries.GetTotalRuneSwapOut(pool, blockTimeStamp)
		//Fees, _ := timeseries.GetFees(pool, blockTimeStamp)
		PoolDeductRefunds, _ := timeseries.GetPoolDeductRefunds(pool,
			blockTimeStamp)
		RuneFeesSwaps, _ := timeseries.GetRuneFeesSwaps(pool, blockTimeStamp)
		PoolDeductSwaps, _ := timeseries.GetPoolDeductSwaps(pool, blockTimeStamp)
		RuneFeeUnstakes, _ := timeseries.GetRuneFeeUnstakes(pool, blockTimeStamp)
		PoolDeductUnstakes, _ := timeseries.GetPoolDeductUnstakes(pool, blockTimeStamp)
		Adds, _ := timeseries.GetAdds(pool, blockTimeStamp)
		Rewards, _ := timeseries.GetAdds(pool, blockTimeStamp)
		//Errata, _ := timeseries.GetErrata(pool,blockTimeStamp)
		Gas, _ := timeseries.GetGas(pool, blockTimeStamp)

		sqlDepth := TotalRuneStakes + TotalRuneSwapIn + Adds + Rewards + Gas
		lessDeductions := sqlDepth - (TotalRunUnstakes  + TotalRuneSwapOut + RuneFeesSwaps + PoolDeductSwaps + PoolDeductUnstakes + RuneFeeUnstakes + PoolDeductRefunds)

		sqlDeepth := strconv.Itoa(int(lessDeductions))
		log.Print("written to csv")
		log.Print(TotalRuneStakes)

		var csvData = [][]string{
			{s, strconv.Itoa(blockTimeStamp), BalanceRune, sqlDeepth},
		}
		err = writer.WriteAll(csvData) // returns error
		if err != nil {
			fmt.Println("An error encountered ::", err)
		}
	}
}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}


func CallAPI(offset string)(BalanceRune string, BalanceAsset string,
	Asset string, PoolUnits string, Status string ){
	resp, err := http.Get( "http://18.159.173.48:1317/thorchain/pool/bnb." +
		"bnb?height="+ offset)

	// check for response error
	if err != nil {
		log.Fatal( err )
	}
	// read response data
	data, _ := ioutil.ReadAll( resp.Body )

	res := poolData{}
	bodyString := string(data)

	json.Unmarshal([]byte(bodyString), &res)
	// close response body
	defer resp.Body.Close()

	return res.BalanceRune, res.BalanceAsset, res.Asset, res.PoolUnits, res.Status

}

func MustLoadConfigFile(path string) *Config {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal("exit on configuration file unavailable: ", err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)

	// prevent config not used due typos
	dec.DisallowUnknownFields()

	var c Config
	if err := dec.Decode(&c); err != nil {
		log.Fatal("exit on malformed configuration: ", err)
	}
	return &c
}

func SetupDatabase(c *Config) {
	db, err := sql.Open("pgx", fmt.Sprintf("user=%s dbname=%s sslmode=%s password=%s host=%s port=%d", c.TimeScale.UserName, c.TimeScale.Database, c.TimeScale.Sslmode, c.TimeScale.Password, c.TimeScale.Host, c.TimeScale.Port))
	if err != nil {
		log.Fatal("exit on PostgreSQL client instantiation: ", err)
	}

	stat.DBQuery = db.QueryContext
	timeseries.DBExec = db.Exec
	timeseries.DBQuery = db.QueryContext
}
// SetupBlockchain launches the synchronisation routine.
func SetupBlockchain(c *Config) <-chan chain.Block {
	// normalize & validate configuration
	if c.ThorChain.NodeURL == "" {
		c.ThorChain.NodeURL = "http://localhost:1317/thorchain"
		log.Printf("default THOR node REST URL to %q", c.ThorChain.NodeURL)
	} else {
		log.Printf("THOR node REST URL is set to %q", c.ThorChain.NodeURL)
	}
	if _, err := url.Parse(c.ThorChain.NodeURL); err != nil {
		log.Fatal("exit on malformed THOR node REST URL: ", err)
	}
	notinchain.BaseURL = c.ThorChain.NodeURL

	if c.ThorChain.URL == "" {
		c.ThorChain.URL = "http://localhost:26657/websocket"
		log.Printf("default Tendermint RPC URL to %q", c.ThorChain.URL)
	} else {
		log.Printf("Tendermint RPC URL is set to %q", c.ThorChain.URL)
	}
	endpoint, err := url.Parse(c.ThorChain.URL)
	if err != nil {
		log.Fatal("exit on malformed Tendermint RPC URL: ", err)
	}

	// instantiate client
	client, err := chain.NewClient(endpoint, c.ThorChain.ReadTimeout.WithDefault(2*time.Second))
	if err != nil {
		// error check does not include network connectivity
		log.Fatal("exit on Tendermint RPC client instantiation: ", err)
	}

	// fetch current position (from commit log)
	offset, _, _, err := timeseries.Setup()
	if err != nil {
		// no point in running without a database
		log.Fatal("exit on RDB unavailable: ", err)
	}
	if offset != 0 {
		offset++
		log.Print("starting with previous blockchain height ", offset)
	}

	var lastNoData atomic.Value
	api.InSync = func() bool {
		return time.Since(lastNoData.Load().(time.Time)) < 2*c.ThorChain.LastChainBackoff.WithDefault(7*time.Second)
	}

	// launch read routine
	ch := make(chan chain.Block, 99)
	go func() {
		backoff := time.NewTicker(c.ThorChain.LastChainBackoff.WithDefault(7 * time.Second))
		defer backoff.Stop()

		// TODO(pascaldekloe): Could use a limited number of
		// retries with skip block logic perhaps?
		for {
			offset, err = client.Follow(ch, offset, nil)
			switch err {
			case chain.ErrNoData:
				lastNoData.Store(time.Now())
			default:
				log.Print("follow blockchain retry on ", err)
			}
			<-backoff.C
		}
	}()

	return ch
}
