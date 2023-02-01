package river

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/juju/errors"
	"github.com/obgnail/mysql-river/config"
)

func NewCanal(host string, port int64, user string, password string) (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", host, port)
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

func NewCanalFromConfig() (*canal.Canal, error) {
	mySQL := config.Config.Mysql
	return NewCanal(mySQL.Host, mySQL.Port, mySQL.User, mySQL.Password)
}

func Run(c *canal.Canal, handler canal.EventHandler) error {
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
