package river

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/juju/errors"
)

func newRiver(addr string, user string, password string) (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = addr
	cfg.User = user
	cfg.Password = password
	cfg.Flavor = "mysql"
	cfg.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return c, nil
}

func RunRiver(addr string, user string, password string, handler canal.EventHandler) error {
	c, err := newRiver(addr, user, password)
	if err != nil {
		return errors.Trace(err)
	}
	coords, err := c.GetMasterPos()
	if err != nil {
		return errors.Trace(err)
	}

	c.SetEventHandler(handler)

	fmt.Println(logo)

	if err := c.RunFrom(coords); err != nil {
		return errors.Trace(err)
	}
	return nil
}

const logo = `
 __    __     __  __     ______     ______     __         ______     __     __   __   ______     ______    
/\ "-./  \   /\ \_\ \   /\  ___\   /\  __ \   /\ \       /\  == \   /\ \   /\ \ / /  /\  ___\   /\  == \   
\ \ \-./\ \  \ \____ \  \ \___  \  \ \ \/\_\  \ \ \____  \ \  __<   \ \ \  \ \ \'/   \ \  __\   \ \  __<   
 \ \_\ \ \_\  \/\_____\  \/\_____\  \ \___\_\  \ \_____\  \ \_\ \_\  \ \_\  \ \__|    \ \_____\  \ \_\ \_\ 
  \/_/  \/_/   \/_____/   \/_____/   \/___/_/   \/_____/   \/_/ /_/   \/_/   \/_/      \/_____/   \/_/ /_/ 
                                                                                                          
`
