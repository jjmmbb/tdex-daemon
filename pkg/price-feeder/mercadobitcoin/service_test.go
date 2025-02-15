package mercadobitcoinfeeder_test

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	pricefeeder "github.com/tdex-network/tdex-daemon/pkg/price-feeder"
	mercadobitcoinfeeder "github.com/tdex-network/tdex-daemon/pkg/price-feeder/mercadobitcoin"

	"github.com/stretchr/testify/require"
)

var (
	// Tickers do MercadoBitcoin (ex: BTC-BRL, ETH-BRL)
	tickers = []string{"BTC-BRL", "ETH-BRL"}
)

func TestService(t *testing.T) {
	feederSvc, err := mercadobitcoinfeeder.NewService()
	require.NoError(t, err)
	require.NotNil(t, feederSvc)

	markets := newMarkets(tickers)
	err = feederSvc.SubscribeMarkets(markets)
	require.NoError(t, err)

	feedCh := feederSvc.Start()
	require.NotNil(t, feedCh)

	wg := &sync.WaitGroup{}
	wg.Add(3)

	go func() {
		defer wg.Done()

		for feed := range feedCh {
			require.NotEmpty(t, feed.Market)
			require.NotEmpty(t, feed.Price)
			fmt.Printf("Received feed: %+v\n", feed)
		}
	}()

	go func() {
		defer wg.Done()

		time.Sleep(10 * time.Second) // Tempo aumentado para coleta de feeds
		feederSvc.Stop()
	}()

	time.Sleep(2 * time.Second) // Espera inicial aumentada

	// Novo mercado para subscribe adicional
	newTicker := []string{"LTC-BRL"}
	mkts := newMarkets(newTicker)
	err = feederSvc.SubscribeMarkets(mkts)
	require.NoError(t, err)
	markets = append(markets, mkts...)

	go func(markets []pricefeeder.Market) {
		defer wg.Done()

		time.Sleep(5 * time.Second) // Tempo ajustado para unsubscribe
		err := feederSvc.UnsubscribeMarkets(markets)
		require.NoError(t, err)
	}(markets)

	wg.Wait()
}

func newMarkets(tickers []string) []pricefeeder.Market {
	markets := make([]pricefeeder.Market, 0, len(tickers))
	for _, ticker := range tickers {
		markets = append(markets, pricefeeder.Market{
			BaseAsset:  randomHex(32),
			QuoteAsset: randomHex(32),
			Ticker:     ticker,
		})
	}
	return markets
}

func randomHex(len int) string {
	return hex.EncodeToString(randomBytes(len))
}

func randomBytes(len int) []byte {
	b := make([]byte, len)
	rand.Read(b)
	return b
}
