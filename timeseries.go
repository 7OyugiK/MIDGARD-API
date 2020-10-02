package timeseries

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

// DBQuery is the SQL client.
var DBQuery func(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

// DBExec is the SQL client.
var DBExec func(query string, args ...interface{}) (sql.Result, error)

// OutboundTimeout is an upperboundary for the amount of time for a followup on outbound events.
const OutboundTimeout = time.Hour

// LastBlockTrack is an in-memory copy of the write state.
var lastBlockTrack atomic.Value

// BlockTrack is a write state.
type blockTrack struct {
	Height    int64
	Timestamp time.Time
	BlockTimestamp int
	Hash      []byte
	aggTrack
}


type midgard struct {
	TotalRuneStakes int64
	TotalRunUnstakes int64
	TotalRuneSwapIn int64
	TotalRuneSwapOut int64
	Fees int64
	PoolDeductRefunds int64
	RuneFeesSwaps int64
	PoolDeductSwaps int64
	RuneFeeUnstakes int64
	PoolDeductUnstakes int64
	Adds int64
	Rewards int64
	Errata int64
	Gas int64
}

// AggTrack has a snapshot of runningTotals.
type aggTrack struct {
	AssetE8DepthPerPool map[string]int64
	RuneE8DepthPerPool  map[string]int64
}

func FetchHeights()([]int64, error) {
	const q = "SELECT height, timestamp, hash, " +
		"agg_state FROM block_log ORDER BY height asc"

	rows, err := DBQuery(context.Background(), q)
	if err != nil {
		return nil, err

	}
	defer rows.Close()

	var track blockTrack

	var offsets []int64


	for rows.Next() {
		var ns int64
		var aggSerial []byte
		rows.Scan(&track.Height, &ns, &track.Hash, &aggSerial)
		track.Timestamp = time.Unix(0, ns)
		if err := gob.NewDecoder(bytes.NewReader(aggSerial)).Decode(&track.aggTrack);
			err != nil {
			return nil, err
		}
		offsets =  append(offsets, track.Height)

	}

	return offsets, rows.Err()

}

func FetchTimestamp(height string)(blockTimestamp int, error error){
	rows, err := DBQuery(context.Background(), "SELECT timestamp FROM block_log where height = $1", height)
	if err != nil {
		return 0000, err
	}
	defer rows.Close()

	var track blockTrack
	if rows.Next() {
		rows.Scan(&track.BlockTimestamp)
	}

	log.Print("timestamp ", track.BlockTimestamp)

	return track.BlockTimestamp, nil

}


func GetTotalRuneStakes(pool string, blockTimeStamp  int)(
	TotalRuneStakes int64, err error){
	rows, err := DBQuery(context.Background(), "select sum(rune_e8) from stake_events where pool = $1 and" +
		" block_timestamp <= $2", pool, blockTimeStamp)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var mg midgard
	if rows.Next() {
		rows.Scan(&mg.TotalRuneStakes)
	}
	log.Print("TotalRuneStakes ", mg.TotalRuneStakes)

	return mg.TotalRuneStakes, nil
}

func GetTotalRunUnstakes(pool string, blockTimeStamp  int)(
	TotalRunUnstakes int64, err error){
	rows, err := DBQuery(context.Background(),  "select sum(oe." +
		"asset_e8) from outbound_events oe join unstake_events ue on (ue." +
		"tx = oe.in_tx) where oe.asset = 'BNB.RUNE-B1A' and ue." +
		"pool = $1 and oe.block_timestamp <= $2", pool, blockTimeStamp)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var mg midgard
	if rows.Next() {
		rows.Scan(&mg.TotalRunUnstakes)
	}
	log.Print("TotalRunUnstakes ", mg.TotalRunUnstakes)

	return mg.TotalRunUnstakes, nil
}

func GetTotalRuneSwapIn(pool string, blockTimeStamp  int)(
	TotalRuneSwapIn int64, err error){

	rows, err := DBQuery(context.Background(),  "select sum(from_e8) from swap_events where pool = $1 and" +
		" from_asset = 'BNB.RUNE-B1A' and block_timestamp <= $2", pool, blockTimeStamp)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var mg midgard
	if rows.Next() {
		rows.Scan(&mg.TotalRuneSwapIn)
	}

	log.Print("TotalRuneSwapIn ", mg.TotalRuneSwapIn)

	return mg.TotalRuneSwapIn, nil
}

func GetTotalRuneSwapOut(pool string, blockTimeStamp  int)(
	TotalRuneSwapOut int64, err error){

	rows, err := DBQuery(context.Background(), "select sum(asset_e8) from outbound_events oe join swap_events" +
		" se on (se.tx = oe.in_tx) where oe.asset = 'BNB.RUNE-B1A' and se." +
		"pool = $1 and se.from_asset != 'BNB.RUNE-B1A' and se." +
		"block_timestamp <= $2", pool, blockTimeStamp)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var mg midgard

	if rows.Next() {
		rows.Scan(&mg.TotalRuneSwapOut)
	}

	log.Print("TotalRuneSwapOut ", mg.TotalRuneSwapOut)

	return mg.TotalRuneSwapOut, nil
}

func GetFees(pool string, blockTimeStamp  int)(Fees int64, err error){
	rows, err := DBQuery(context.Background(), "select sum(asset_e8) from fee_events fe join swap_events se on" +
		" (se.tx = fe.tx) where fe.asset = 'BNB.RUNE-B1A' and se." +
		"pool = $1 and se.block_timestamp <= $2", pool, blockTimeStamp)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var mg midgard
	if rows.Next() {
		rows.Scan(&mg.Fees)
	}
	log.Print("Fees ", mg.Fees)

	return mg.Fees, nil
}

func GetPoolDeductRefunds(pool string, blockTimeStamp  int)(
	PoolDeductRefunds int64, err error){
	rows, err := DBQuery(context.Background(), "select sum(pool_deduct) from fee_events fe join refund_events" +
		" re on (re.tx = fe.tx) where fe.asset != 'BNB.RUNE-B1A' and re." +
		"asset = $1 and re.block_timestamp <= $2", pool, blockTimeStamp)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var mg midgard
	if rows.Next() {
		rows.Scan(&mg.PoolDeductRefunds)
	}
	log.Print("PoolDeductRefunds ", mg.PoolDeductRefunds)

	return mg.PoolDeductRefunds, nil
}

func GetRuneFeesSwaps(pool string, blockTimeStamp  int)(
	RuneFeesSwaps int64, err error){

	rows, err := DBQuery(context.Background(), "select sum(asset_e8) from fee_events fe join swap_events se on" +
		" (se.tx = fe.tx) where fe.asset = 'BNB.RUNE-B1A' and se." +
		"pool = $1 and se.block_timestamp <= $2", pool, blockTimeStamp)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var mg midgard
	if rows.Next() {
		rows.Scan(&mg.RuneFeesSwaps)
	}

	log.Print("RuneFeesSwaps ", mg.RuneFeesSwaps)

	return mg.RuneFeesSwaps, nil
}

func GetPoolDeductSwaps(pool string, blockTimeStamp  int)(
	PoolDeductSwaps int64, err error){
	rows, err := DBQuery(context.Background(), "select sum(pool_deduct) from fee_events fe join swap_events se" +
		" on (se.tx = fe.tx) where fe.asset != 'BNB.RUNE-B1A' and se." +
		"pool = $1 and se.block_timestamp <= $2", pool, blockTimeStamp)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var mg midgard
	if rows.Next() {
		rows.Scan(&mg.PoolDeductSwaps)
	}

	log.Print("PoolDeductSwaps ", mg.PoolDeductSwaps)

	return mg.PoolDeductSwaps, nil
}

func GetRuneFeeUnstakes(pool string, blockTimeStamp  int)(
	RuneFeeUnstakes int64, err error){
	rows, err := DBQuery(context.Background(), "select sum(fe.asset_e8) from fee_events fe join unstake_events" +
		" ue on (ue.tx = fe.tx) where fe.asset = 'BNB.RUNE-B1A' and ue." +
		"pool = $1 and fe.block_timestamp <= $2", pool, blockTimeStamp)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var mg midgard
	if rows.Next() {
		rows.Scan(&mg.RuneFeeUnstakes)
	}
	log.Print("RuneFeeUnstakes ", mg.RuneFeeUnstakes)

	return mg.RuneFeeUnstakes, nil
}

func GetPoolDeductUnstakes(pool string, blockTimeStamp  int)(
	PoolDeductUnstakes int64, err error){
	rows, err := DBQuery(context.Background(),  "select sum(pool_deduct) from fee_events fe join unstake_events" +
		" ue on (ue.tx = fe.tx) where fe.asset != 'BNB.RUNE-B1A' and ue." +
		"pool = 'BNB.BNB' and fe.block_timestamp <= $2", pool, blockTimeStamp)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var mg midgard
	if rows.Next() {
		rows.Scan(&mg.PoolDeductUnstakes)
	}

	log.Print("PoolDeductUnstakes ", mg.PoolDeductUnstakes)

	return mg.PoolDeductRefunds, nil
}

func GetAdds(pool string, blockTimeStamp  int) (Add int64, err error) {
	rows, err := DBQuery(context.Background(), "select sum(rune_e8) from add_events where pool = $1 and" +
		" block_timestamp <=  $2", pool, blockTimeStamp)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var mg midgard
	if rows.Next() {
		rows.Scan(&mg.Adds)
	}
	log.Print("Adds ", mg.Adds)

	return mg.Rewards, nil
}

func GetRewards(pool string, blockTimeStamp  int)(Rewards int64, err error)  {
	rows, err := DBQuery(context.Background(), "select sum(rune_e8) from rewards_event_entries where pool" +
		" = $1 and block_timestamp <=  $2", pool, blockTimeStamp)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var mg midgard
	if rows.Next() {
		rows.Scan(&mg.Rewards)
	}

	log.Print("Rewards ", mg.Rewards)

	return mg.Rewards, nil
}

func GetErrata(pool string, blockTimeStamp  int)(Errata int64, err error){
	rows, err := DBQuery(context.Background(), "select sum(rune_e8) from errata_events where asset = $1 and" +
		" block_timestamp <=  $2", pool, blockTimeStamp)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var mg midgard
	if rows.Next() {
		rows.Scan(&mg.Errata)
	}

	log.Print("Errata ", mg.Errata)

	return mg.Errata, nil
}

func GetGas(pool string, blockTimeStamp  int) (Gas int64, err error) {
	rows, err := DBQuery(context.Background(), "select sum(rune_e8) from gas_events where asset = $1 and" +
		" block_timestamp <= $2", pool, blockTimeStamp)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var mg midgard
	if rows.Next() {
		rows.Scan(&mg.Gas)
	}

	log.Print("Gas ", mg.Gas)
	
	return mg.Gas, nil
}


// Setup initializes the package. The previous state is restored (if there was any).
func Setup() (lastBlockHeight int64, lastBlockTimestamp time.Time, lastBlockHash []byte, err error) {
	const q = "SELECT height, timestamp, hash, agg_state FROM block_log ORDER BY height DESC LIMIT 1"
	rows, err := DBQuery(context.Background(), q)
	if err != nil {
		return 0, time.Time{}, nil, fmt.Errorf("last block lookup: %w", err)
	}
	defer rows.Close()

	var track blockTrack
	if rows.Next() {
		var ns int64
		var aggSerial []byte
		rows.Scan(&track.Height, &ns, &track.Hash, &aggSerial)
		track.Timestamp = time.Unix(0, ns)
		if err := gob.NewDecoder(bytes.NewReader(aggSerial)).Decode(&track.aggTrack);
		err != nil {
			return 0, time.Time{}, nil, fmt.Errorf("restore with malformed aggregation state denied on %w", err)
		}
	}

	// sync in-memory tracker
	lastBlockTrack.Store(&track)

	// apply aggregation state to recorder
	for pool, E8 := range track.AssetE8DepthPerPool {
		v := E8 // copy
		recorder.assetE8DepthPerPool[pool] = &v
	}
	for pool, E8 := range track.RuneE8DepthPerPool {
		v := E8 // copy
		recorder.runeE8DepthPerPool[pool] = &v
	}

	return track.Height, track.Timestamp, track.Hash, rows.Err()
}

// CommitBlock marks the given height as done.
// Invokation of EventListener during CommitBlock causes race conditions!
func CommitBlock(height int64, timestamp time.Time, hash []byte) error {
	// in-memory snapshot
	track := blockTrack{
		Height:    height,
		Timestamp: timestamp,
		Hash:      make([]byte, len(hash)),
		aggTrack: aggTrack{
			AssetE8DepthPerPool: recorder.AssetE8DepthPerPool(),
			RuneE8DepthPerPool:  recorder.RuneE8DepthPerPool(),
		},
	}
	copy(track.Hash, hash)

	// persist to database
	var aggSerial bytes.Buffer
	if err := gob.NewEncoder(&aggSerial).Encode(&track.aggTrack); err != nil {
		// won't bing the service down, but prevents state recovery
		log.Print("aggregation state ommited from persistence:", err)
	}
	const q = "INSERT INTO block_log (height, timestamp, hash, agg_state) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING"
	result, err := DBExec(q, height, timestamp.UnixNano(), hash, aggSerial.Bytes())
	if err != nil {
		return fmt.Errorf("persist block height %d: %w", height, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("persist block height %d result: %w", height, err)
	}
	if n == 0 {
		log.Printf("block height %d already committed", height)
	}

	// calculate & reset
	recorder.linkedEvents.ApplyOutboundQ(&recorder.runningTotals, height, timestamp)
	recorder.linkedEvents.ApplyFeeQ(&recorder.runningTotals, height, timestamp)

	// commit in-memory state
	lastBlockTrack.Store(&track)

	return nil
}

// LastBlock gets the most recent commit.
func LastBlock() (height int64, timestamp time.Time, hash []byte) {
	track := lastBlockTrack.Load().(*blockTrack)
	return track.Height, track.Timestamp, track.Hash
}

// AssetAndRuneDepths gets the current snapshot handle.
// The asset price is the asset depth divided by the RUNE depth.
func AssetAndRuneDepths() (assetE8PerPool, runeE8PerPool map[string]int64, timestamp time.Time) {
	track := lastBlockTrack.Load().(*blockTrack)
	return track.aggTrack.AssetE8DepthPerPool, track.aggTrack.RuneE8DepthPerPool, track.Timestamp
}
