package completion

import (
	"errors"
	"os"

	"github.com/QubitProducts/logspray/cmd/logs/root"
	"github.com/spf13/cobra"
)

func init() {
	bash = compCmd.Flags().BoolP("bash", "b", false, "bash completion")
	zsh = compCmd.Flags().BoolP("zsh", "z", false, "zsh completion")
	root.RootCmd.AddCommand(compCmd)
}

var (
	bash *bool
	zsh  *bool
)

var compCmd = &cobra.Command{
	Use:   "completion",
	Short: "Command line completion files for logspray",
	RunE: func(*cobra.Command, []string) error {
		if *bash && *zsh {
			return errors.New("please pick one of zsh or bash")
		}
		if *zsh {
			root.RootCmd.GenZshCompletion(os.Stdout)
		}
		return root.RootCmd.GenBashCompletion(os.Stdout)
	},
}
