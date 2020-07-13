package main

import (
	"bytes"
	"flag"
	"fmt"
	binidl "github.com/efficient/gobin-codegen/src/binidl"
	"go/build"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var (
	pname   = flag.String("n", "", "New protocol name")
	msgFlow = flag.String("msg", "", "Go file with message definitions")
	shpath  = flag.String("dir", "", "Path to the shreplic root")
	remove  = flag.Bool("rm", false, "Remove the protocol")
)

func main() {
	flag.Parse()

	if *pname == "" {
		if !*remove {
			fmt.Println("cannot initialize nameless protocol")
		} else {
			fmt.Println("cannot remove nameless protocol")
		}
		return
	}

	if *msgFlow == "" && !*remove {
		fmt.Println("please provide a message definition source file")
		return
	}

	if _, err := os.Stat(*msgFlow); os.IsNotExist(err) {
		fmt.Printf("%s: no such file\n", *msgFlow)
		return
	}

	if *shpath == "" {
		*shpath = path()
	}

	var (
		buf         bytes.Buffer
		protocolDir = *shpath + "/user/" + *pname
		defsFile    = *shpath + "/user/" + *pname + "/defs.go"
		protoFile   = *shpath + "/user/" + *pname + "/proto.go"
		replicaFile = *shpath + "/user/" + *pname + "/" + *pname + ".go"
	)

	if *remove {
		err := removeProtocol(protocolDir,
			*shpath+"/server/server.go", *pname)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(*pname, "is successfully deleted")
		}
		return
	}

	if _, err := os.Stat(protocolDir); !os.IsNotExist(err) {
		fmt.Printf("%s: the protocol name is already taken\n", *pname)
		return
	}

	if _, err := os.Stat(protocolDir); os.IsNotExist(err) {
		err := os.Mkdir(protocolDir, os.ModePerm)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	defs, ms, err := getDefs(*msgFlow)
	if err != nil {
		fmt.Println(err)
		return
	}
	bi := binidl.NewBinidl(*msgFlow, false)
	if bi == nil {
		return
	}
	proto := execAndOutToString(bi.PrintGo)
	replica := protocol(*pname, ms)
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "", replica, parser.ParseComments)
	if err != nil {
		fmt.Println(err)
		return
	}
	printer.Fprint(&buf, fset, f)
	replica = buf.String()

	err = ioutil.WriteFile(defsFile, []byte(defs), 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = ioutil.WriteFile(protoFile, []byte(proto), 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = ioutil.WriteFile(replicaFile, []byte(replica), 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	make := "all:\n"
	make = make + "\tmake -C " + *shpath + "\n"
	make = make + "\tln -fs " + *shpath + "/bin ./\n"
	err = ioutil.WriteFile(protocolDir+"/Makefile", []byte(make), 0644)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = updateServer(*shpath+"/server/server.go", *pname, false)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = os.Symlink(protocolDir, "./"+*pname)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(*pname, "is successfully initialized")
	fmt.Println("check", protocolDir, "for the associated source files")
}

func protocol(name string, msgs []string) string {
	p := "package " + name + "\n\n"

	p = p + "import (\n"
	p = p + "\"time\"\n\n"
	p = p + "\"github.com/vonaka/shreplic/server/smr\"\n"
	p = p + "\"github.com/vonaka/shreplic/tools/fastrpc\"\n"
	p = p + ")\n\n"

	p = p + "type Replica struct {\n"
	p = p + "*smr.Replica\n"
	p = p + "cs CommunicationSupply\n\n"
	p = p + "//...\n"
	p = p + "}\n\n"

	p = p + "type CommunicationSupply struct {\n"
	p = p + "maxLatency time.Duration\n\n"
	for _, msg := range msgs {
		p = p + msg + "Chan chan fastrpc.Serializable\n"
	}
	p = p + "\n"
	for _, msg := range msgs {
		p = p + msg + "RPC uint8\n"
	}
	p = p + "}\n\n"

	p = p + "func NewReplica"
	p = p + "(id, f int, addrs []string, thrifty, exec, lread, drep bool, args string)"
	p = p + " *Replica {\n"
	p = p + "r := &Replica{\n"
	p = p + "Replica: smr.NewReplica(id, f, addrs, thrifty, exec, lread, drep),\n\n"
	p = p + "//...\n\n"
	p = p + "cs: CommunicationSupply{\n"
	p = p + "maxLatency: 0,\n\n"
	for _, msg := range msgs {
		p = p + msg + "Chan: make(chan fastrpc.Serializable, smr.CHAN_BUFFER_SIZE),\n"
	}
	p = p + "},\n"
	p = p + "}\n\n"

	for _, msg := range msgs {
		p = p + "r.cs." + msg + "RPC = r.RPC.Register(new(" + msg + "), r.cs." + msg + "Chan)\n"
	}
	p = p + "\n\n"
	p = p + "go r.run()\n\n"
	p = p + "return r\n"
	p = p + "}\n\n"

	p = p + "func (r *Replica) run() {\n"
	p = p + "r.ConnectToPeers()\n"
	p = p + "latencies := r.ComputeClosestPeers()\n"
	p = p + "for _, l := range latencies {\n"
	p = p + "d := time.Duration(l*1000*1000) * time.Nanosecond\n"
	p = p + "if d > r.cs.maxLatency {\n"
	p = p + "r.cs.maxLatency = d\n"
	p = p + "}\n"
	p = p + "}\n\n"
	p = p + "go r.WaitForClientConnections()\n\n"
	p = p + "for !r.Shutdown {\n"
	p = p + "select {\n"
	p = p + "case propose := <-r.ProposeChan:\n"
	p = p + "r.handlePropose(propose)\n"
	for _, msg := range msgs {
		p = p + "\ncase m := <-r.cs." + msg + "Chan:\n"
		p = p + "v" + msg + " := m.(*" + msg + ")\n"
		p = p + "r.handle" + msg + "(v" + msg + ")\n"
	}
	p = p + "}\n"
	p = p + "}\n"
	p = p + "}\n\n"

	p = p + "func (r *Replica) handlePropose(propose *smr.GPropose) {\n"
	p = p + "//...\n"
	p = p + "}\n"

	for _, msg := range msgs {
		p = p + "\nfunc (r *Replica) handle" + msg + "(msg *" + msg + ") {\n"
		p = p + "//...\n"
		p = p + "}\n"
	}

	for _, msg := range msgs {
		p = p + "\nfunc (m *" + msg + ") New() fastrpc.Serializable {\n"
		p = p + "\treturn new(" + msg + ")\n"
		p = p + "}\n"
	}

	return p
}

func updateServer(filename, pname string, remove bool) error {
	s, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	server := string(s)

	imp := "//user imports\n"
	impm := "\t\"github.com/vonaka/shreplic/user/" + pname + "\""
	imp = imp + impm
	if !remove {
		server = strings.Replace(server, "//user imports", imp, 1)
	} else {
		server = strings.Replace(server, impm+"\n", "", 1)
	}

	flg := "//user flags\n"
	flgn := "\tdo" + pname + " = flag.Bool(\"" + pname +
		"\", false, \"Use " + pname + " as the replication protocol\")"
	flg = flg + flgn
	if !remove {
		server = strings.Replace(server, "//user flags", flg, 1)
	} else {
		server = strings.Replace(server, flgn+"\n", "", 1)
	}

	calln := "\t} else if *do" + pname + " {\n"
	calln = calln + "\t\tlog.Println(\"Starting " + pname + " replica...\")\n"
	calln = calln + "\t\trep := " + pname +
		".NewReplica(replicaId, *maxfailures, nodeList, " +
		"*thrifty, *exec, *lread, *dreply, *args)\n"
	calln = calln + "\t\trpc.Register(rep)\n"
	call := calln + "\t} else if *doPaxoi {"
	if !remove {
		server = strings.Replace(server, "\t} else if *doPaxoi {", call, 1)
	} else {
		server = strings.Replace(server, calln, "", 1)
	}

	return ioutil.WriteFile(filename, []byte(server), 0644)
}

func removeProtocol(dir, serverf, pname string) error {
	err := os.RemoveAll(dir)
	if err != nil {
		return err
	}
	err = os.RemoveAll("./" + pname)
	if err != nil {
		return err
	}
	return updateServer(serverf, pname, true)
}

func getDefs(filename string) (string, []string, error) {
	defs, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", nil, err
	}
	sdefs := string(defs)
	return sdefs, msgs(sdefs), err
}

func msgs(source string) []string {
	ms := []string{}
	for _, t := range strings.Split(source, "type ") {
		fs := strings.Fields(t)
		if len(fs) < 1 {
			continue
		}
		if strings.HasPrefix(fs[0], "M") {
			ms = append(ms, fs[0])
		}
	}
	return ms
}

func path() string {
	bin, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	dir := filepath.Dir(bin)

	if _, err := os.Stat(dir + "/user/"); os.IsNotExist(err) {
		err := os.Mkdir(dir + "/user/", os.ModePerm)
		if err != nil {
			gopath := os.Getenv("GOPATH")
			if gopath == "" {
				gopath = build.Default.GOPATH
			}
			dir = gopath + "src/github.com/vonaka/shreplic"
			if _, err := os.Stat(dir + "/user/"); os.IsNotExist(err) {
				err := os.Mkdir(dir + "/user/", os.ModePerm)
				if err != nil {
					log.Fatal("can't find shreplic source")
				}
			}
		}
	}

	return dir
}

func execAndOutToString(f func()) string {
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		log.Fatal(err)
	}
	os.Stdout = w

	f()

	out := make(chan string)
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		out <- buf.String()
	}()

	w.Close()
	os.Stdout = oldStdout
	return <-out
}
