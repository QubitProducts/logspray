package completion

import (
	"os"

	"github.com/QubitProducts/logspray/cmd/logs/root"
	"github.com/spf13/cobra"
)

func init() {
	root.RootCmd.AddCommand(compCmd)
}

var compCmd = &cobra.Command{
	Use:   "completion",
	Short: "A brief description of your application",
	RunE: func(*cobra.Command, []string) error {
		return root.RootCmd.GenBashCompletion(os.Stdout)
	},
}
