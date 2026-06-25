package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const prompt = "> "

func main() {
	// listen 是本机接收端口，peer 是对方的接收端口。
	// 两个程序互相把自己的 listen 地址填到对方的 peer，就能同时收发。
	listenAddr := flag.String("listen", "127.0.0.1:9000", "local TCP address to receive on")
	peerAddr := flag.String("peer", "", "peer TCP address to send to")
	retryInterval := flag.Duration("retry", time.Second, "connect retry interval")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if strings.TrimSpace(*peerAddr) == "" {
		log.Fatal("missing -peer; example: go run . -listen 127.0.0.1:9000 -peer 127.0.0.1:9001")
	}

	if err := runDualLine(*listenAddr, *peerAddr, *retryInterval); err != nil {
		log.Fatal(err)
	}
}

func runDualLine(listenAddr, peerAddr string, retryInterval time.Duration) error {
	// 先启动接收线：监听本地端口，等待对方连接进来。
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", listenAddr, err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inboundCh := make(chan net.Conn, 1)
	acceptErrCh := make(chan error, 1)
	outboundCh := make(chan net.Conn, 1)

	log.Printf("receiving on %s", listenAddr)
	go acceptOne(ctx, listener, inboundCh, acceptErrCh)

	// 同时启动发送线：主动连接对方的接收端口，失败时会自动重试。
	log.Printf("sending to %s", peerAddr)
	go dialUntilConnected(ctx, peerAddr, retryInterval, outboundCh)

	// 等到接收线和发送线都建立成功后，才进入真正的聊天收发逻辑。
	var inbound net.Conn
	var outbound net.Conn
	for inbound == nil || outbound == nil {
		select {
		case conn := <-inboundCh:
			inbound = conn
			log.Printf("receive line connected from %s", conn.RemoteAddr())
		case conn := <-outboundCh:
			outbound = conn
			log.Printf("send line connected to %s", conn.RemoteAddr())
		case err := <-acceptErrCh:
			return err
		}
	}

	cancel()
	return handleLines(inbound, outbound)
}

func acceptOne(ctx context.Context, listener net.Listener, connCh chan<- net.Conn, errCh chan<- error) {
	// Accept 会阻塞等待对方连接；收到连接后交给主流程。
	conn, err := listener.Accept()
	if err != nil {
		select {
		case <-ctx.Done():
			return
		case errCh <- fmt.Errorf("accept connection: %w", err):
			return
		}
	}

	select {
	case connCh <- conn:
	case <-ctx.Done():
		_ = conn.Close()
	}
}

func dialUntilConnected(ctx context.Context, addr string, retryInterval time.Duration, connCh chan<- net.Conn) {
	// 对方可能还没启动，所以这里持续重连，直到连接成功或程序退出。
	if retryInterval <= 0 {
		retryInterval = time.Second
	}

	for {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err == nil {
			select {
			case connCh <- conn:
			case <-ctx.Done():
				_ = conn.Close()
			}
			return
		}

		log.Printf("connect to %s failed: %v; retrying in %s", addr, err, retryInterval)
		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func handleLines(inbound, outbound net.Conn) error {
	// inbound 专门负责接收，outbound 专门负责发送，这就是“双线”。
	defer inbound.Close()
	defer outbound.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 2)
	var closeOnce sync.Once
	// 任意一条线退出时，只关闭一次，避免两个 goroutine 同时重复关闭连接。
	closeConn := func() {
		closeOnce.Do(func() {
			cancel()
			_ = inbound.Close()
			_ = outbound.Close()
		})
	}

	// 一个 goroutine 负责从网络读消息，一个 goroutine 负责从键盘读输入并发送。
	go receiveLoop(ctx, inbound, errCh, closeConn)
	go sendLoop(ctx, outbound, os.Stdin, errCh, closeConn)

	err := <-errCh
	closeConn()

	if err != nil && !errors.Is(err, io.EOF) && !isClosedNetworkError(err) {
		return err
	}
	log.Println("lines closed")
	return nil
}

func receiveLoop(ctx context.Context, conn net.Conn, errCh chan<- error, closeConn func()) {
	// 按行读取对方发来的消息，读到换行符才显示一条完整消息。
	reader := bufio.NewReader(conn)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		message, err := reader.ReadString('\n')
		if err != nil {
			errCh <- fmt.Errorf("receive failed: %w", err)
			closeConn()
			return
		}

		fmt.Printf("\rpeer: %s%s", message, prompt)
	}
}

func sendLoop(ctx context.Context, conn net.Conn, input io.Reader, errCh chan<- error, closeConn func()) {
	// 从标准输入读取用户输入，每输入一行就发送给对方。
	scanner := bufio.NewScanner(input)
	writer := bufio.NewWriter(conn)

	fmt.Print(prompt)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		text := scanner.Text()
		// 输入 /quit 时主动退出，并关闭收发两条线。
		if strings.EqualFold(strings.TrimSpace(text), "/quit") {
			errCh <- nil
			closeConn()
			return
		}

		if _, err := writer.WriteString(text + "\n"); err != nil {
			errCh <- fmt.Errorf("send failed: %w", err)
			closeConn()
			return
		}
		if err := writer.Flush(); err != nil {
			errCh <- fmt.Errorf("flush failed: %w", err)
			closeConn()
			return
		}

		fmt.Print(prompt)
	}

	if err := scanner.Err(); err != nil {
		errCh <- fmt.Errorf("read stdin failed: %w", err)
	} else {
		errCh <- nil
	}
	closeConn()
}

func isClosedNetworkError(err error) bool {
	// 主动关闭连接时，不同系统返回的错误文本可能不同，这里统一当作正常退出。
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "An existing connection was forcibly closed")
}
