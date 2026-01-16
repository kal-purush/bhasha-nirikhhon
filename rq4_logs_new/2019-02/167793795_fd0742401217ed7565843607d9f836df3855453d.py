class Argument:
    def get_argument(self):
        """
        get argument at CLI

        Returns
        -------
        args : list
            argument list
        """
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('function', help=('Function to call: add, list, complete'))
        parser.add_argument('-p', '--project', help="project to add task to")
        parser.add_argument('-c', '--content', help="desired task, or query")
        parser.add_argument('-d', '--due', help="Due when?")
        args = parser.parse_args()
        return args