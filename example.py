import luigi
import xrootd

EOS_HOME = '/eos/lhcb/user/i/ibabusch'
DEFAULT_SERVER = 'eoslhcb.cern.ch'

class MakeNumbers(luigi.Task):
    outpath = luigi.Parameter()
    server = luigi.Parameter(default=DEFAULT_SERVER)

    def output(self):
        return xrootd.XRootDTarget('root://' + self.server + '//' + self.outpath)

    def require(self):
        return []

    def run(self):
        f = self.output().open('w')

        for i in range(1, 101):
            resp, _ = f.write('{}\n'.format(i))
            if resp.error:
                raise ValueError(str(resp))

        f.close()

class SumNumbers(luigi.Task):
    outpath = luigi.Parameter()
    server = luigi.Parameter(default=DEFAULT_SERVER)

    def output(self):
        return xrootd.XRootDTarget('root://' + self.server + '//' + self.outpath)

    def requires(self):
        return MakeNumbers(EOS_HOME + '/numbers')

    def run(self):

        f = self.input().open('r')
        g = self.output().open('w')

        result = sum(map(int, f.readlines()))
        g.write(str(result) + '\n')

        g.close()
        f.close()

if __name__ == '__main__':
    luigi.run()

