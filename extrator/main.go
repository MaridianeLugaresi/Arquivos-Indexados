package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
)

type storage struct {
	counter            int64
	coordinatesCounter int64
	actualDate         time.Time
	file               *os.File
}

func main() {
	flags := flag.NewFlagSet("user-auth", flag.ExitOnError)
	consumerKey := flags.String("consumer-key", os.Getenv("TWITTER_CONSUMER_KEY"), "Twitter Consumer Key")
	consumerSecret := flags.String("consumer-secret", os.Getenv("TWITTER_CONSUMER_SECRET"), "Twitter Consumer Secret")
	accessToken := flags.String("access-token", os.Getenv("TWITTER_ACCESS_TOKEN"), "Twitter Access Token")
	accessSecret := flags.String("access-secret", os.Getenv("TWITTER_ACCESS_SECRET"), "Twitter Access Secret")
	flags.Parse(os.Args[1:])

	if *consumerKey == "" || *consumerSecret == "" || *accessToken == "" || *accessSecret == "" {
		log.Fatal("Consumer key/secret and Access token/secret required")
	}

	config := oauth1.NewConfig(*consumerKey, *consumerSecret)
	token := oauth1.NewToken(*accessToken, *accessSecret)
	// OAuth1 http.Client will automatically authorize Requests
	httpClient := config.Client(oauth1.NoContext, token)

	// Twitter Client
	client := twitter.NewClient(httpClient)
	storage := &storage{}
	defer storage.close()

	// Convenience Demux demultiplexed stream messages
	demux := twitter.NewSwitchDemux()
	demux.Tweet = func(tweet *twitter.Tweet) {
		storage.save(tweet)
	}
	demux.DM = func(dm *twitter.DirectMessage) {
		fmt.Println(dm.SenderID)
	}
	demux.Event = func(event *twitter.Event) {
		fmt.Printf("%#v\n", event)
	}

	fmt.Println("Starting Stream...")

	// FILTER
	filterParams := &twitter.StreamFilterParams{
		Track:         []string{"bolsonaro", "#bolsonaro", "governo"},
		StallWarnings: twitter.Bool(true),
	}
	stream, err := client.Streams.Filter(filterParams)
	if err != nil {
		log.Fatal(err)
	}

	// Receive messages until stopped or stream quits
	go demux.HandleChan(stream.Messages)
	// show message infos
	go storage.showInfos()

	// Wait for SIGINT and SIGTERM (HIT CTRL-C)
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println(<-ch)

	fmt.Println("Stopping Stream...")
	stream.Stop()
}

func (s *storage) save(tweet *twitter.Tweet) {
	atomic.AddInt64(&s.counter, 1)
	if tweet.Coordinates != nil {
		atomic.AddInt64(&s.coordinatesCounter, 1)
	}

	if !time.Now().Truncate(24 * time.Hour).Equal(s.actualDate) {
		s.rotate()
	}
	err := json.NewEncoder(s.file).Encode(tweet)
	if err != nil {
		log.Printf("[ERROR] Failed to encode tweet, err: %v", err)
	}
}

func (s *storage) showInfos() {
	c := time.Tick(time.Minute)
	for range c {
		log.Printf("[INFO] %d tweets imported and %d tweets with locations",
			atomic.LoadInt64(&s.counter), atomic.LoadInt64(&s.coordinatesCounter))
	}
}

func (s *storage) rotate() {
	s.actualDate = time.Now().Truncate(24 * time.Hour)
	fileName := fmt.Sprintf("data/%s.data", s.actualDate.Format("20060102"))
	log.Printf("[INFO] rotation file to %s", fileName)
	err := s.file.Close()
	if err != nil {
		log.Printf("[ERROR] failed to close the file for truncation, err: %s", err)
	}
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("[ERROR] failed to create export file. err: %s", err)
	}
	s.file = file
}

func (c *storage) close() {
	err := c.file.Close()
	if err != nil {
		log.Printf("[ERROR] failed to close the file, err: %s", err)
	}
}