"""

Name:    idxbpnn.py
Purpose: neural net processor library

"""

# Back-Propagation Neural Networks
#
# Written in Python.  See http://www.python.org/
# Placed in the public domain.
# Neil Schemenauer <nas@arctrix.com>

import math
import random
import string
import pickle
import sys

random.seed(0)

# calculate a random number where:  a <= rand < b
def rand(a, b):
    return (b-a)*random.random() + a

# Make a matrix (we could use NumPy to speed this up)
def makeMatrix(I, J, fill=0.0):
    m = []
    for i in range(I):
        m.append([fill]*J)
    return m

# our sigmoid function, tanh is a little nicer than the standard 1/(1+e^-x)
def sigmoid(x):
    return math.tanh(x)

# derivative of our sigmoid function
def dsigmoid(y):
    return 1.0-y*y

class NN:

    def __init__(self, ni, nh, no):
        self.nnfileid="idx nnfile v1"
        self.nnfileext="onn"

        # number of input, hidden, and output nodes
        self.ni = ni + 1 # +1 for bias node
        self.nh = nh
        self.no = no

        # activations for nodes
        self.ai = [1.0]*self.ni
        self.ah = [1.0]*self.nh
        self.ao = [1.0]*self.no

        # create weights
        self.wi = makeMatrix(self.ni, self.nh)
        self.wo = makeMatrix(self.nh, self.no)
        # set them to random vaules
        for i in range(self.ni):
            for j in range(self.nh):
                self.wi[i][j] = rand(-2.0, 2.0)
        for j in range(self.nh):
            for k in range(self.no):
                self.wo[j][k] = rand(-2.0, 2.0)

        # last change in weights for momentum
        self.ci = makeMatrix(self.ni, self.nh)
        self.co = makeMatrix(self.nh, self.no)

    def update(self, inputs):
        if len(inputs) != self.ni-1:
            raise ValueError, 'wrong number of inputs'

        # input activations
        for i in range(self.ni-1):
            #self.ai[i] = sigmoid(inputs[i])
            self.ai[i] = inputs[i]

        # hidden activations
        for j in range(self.nh):
            sum = 0.0
            for i in range(self.ni):
                sum = sum + self.ai[i] * self.wi[i][j]
            self.ah[j] = sigmoid(sum)

        # output activations
        for k in range(self.no):
            sum = 0.0
            for j in range(self.nh):
                sum = sum + self.ah[j] * self.wo[j][k]
            self.ao[k] = sigmoid(sum)

        return self.ao[:]


    def backPropagate(self, targets, N, M):
        if len(targets) != self.no:
            raise ValueError, 'wrong number of target values'

        # calculate error terms for output
        output_deltas = [0.0] * self.no
        for k in range(self.no):
            error = targets[k]-self.ao[k]
            output_deltas[k] = dsigmoid(self.ao[k]) * error

        # calculate error terms for hidden
        hidden_deltas = [0.0] * self.nh
        for j in range(self.nh):
            error = 0.0
            for k in range(self.no):
                error = error + output_deltas[k]*self.wo[j][k]
            hidden_deltas[j] = dsigmoid(self.ah[j]) * error

        # update output weights
        for j in range(self.nh):
            for k in range(self.no):
                change = output_deltas[k]*self.ah[j]
                self.wo[j][k] = self.wo[j][k] + N*change + M*self.co[j][k]
                self.co[j][k] = change
                #print N*change, M*self.co[j][k]

        # update input weights
        for i in range(self.ni):
            for j in range(self.nh):
                change = hidden_deltas[j]*self.ai[i]
                self.wi[i][j] = self.wi[i][j] + N*change + M*self.ci[i][j]
                self.ci[i][j] = change

        # calculate error
        error = 0.0
        for k in range(len(targets)):
            error = error + 0.5*(targets[k]-self.ao[k])**2
        return error


    def test(self, patterns):
        for p in patterns:
            print p[0], '->', self.update(p[0])

    def weights(self):
        print 'Input weights:'
        for i in range(self.ni):
            print self.wi[i]
        print
        print 'Output weights:'
        for j in range(self.nh):
            print self.wo[j]

    def train(self, patterns, tstpatterns, iterations=1000, N=0.5, M=0.1):
        # N: learning rate
        # M: momentum factor
        saved_error=0.0
        for i in xrange(iterations):
            error = 0.0
            for p in patterns:
                inputs = p[0]
                targets = p[1]
                self.update(inputs)
                error = error + self.backPropagate(targets, N, M)
            if i % 100 == 0:
                #print 'error %-14f' % error
                tsterror = 0.0
                for p in tstpatterns:
                    #print "p[0]: ", p[0]
                    #print "p[1]: ", p[1][0]
                    inputs = p[0]
                    target = p[1]
                    #print self.update(p[0])
                    #print self.update(p[0])[0]
                    tsterror = tsterror + 0.5*(p[1][0]-self.update(p[0])[0])**2
                if saved_error<>0:
                    percent_error=(saved_error-tsterror)/saved_error*100
                else:
                    percent_error=100    # set to large value so the
                print 'test pattern error %-14f' % tsterror
                print '%-5f' % percent_error
                saved_error=tsterror

    def trainnet(self, trnpatterns, tstpatterns, N, M):
        self.train(trnpatterns,tstpatterns,10000,N,M)
        # need to take tstpatterns and do a xxx.test(tstpatterns) and calculate error.


    def save(self, file):
        self.nnfilename=file + "." + self.nnfileext
        nfile=open(self.nnfilename, "w")
        pickle.dump(self.nnfileid, nfile)
        pickle.dump(self.wi, nfile)
        pickle.dump(self.wo, nfile)
        nfile.close()
        # test ability to read file
       #mfile=open(nnfilename, "r")
       #self.wi=pickle.load(mfile)
       # self.wo=pickle.load(mfile)
       #mfile.close()

    def restore(self, file):
        self.nnfilename=file + "." + self.nnfileext
        mfile=open(self.nnfilename, "r")
        fileid=pickle.load(mfile)
        if fileid != self.nnfileid:
            print "ERROR: fileid 'idx nnfile vX' not found on file, wrong file type(bad magic number), found: ", fileid
            sys.exit(1)
        self.wi=pickle.load(mfile)
        self.wo=pickle.load(mfile)
        mfile.close()

    def runnet(self, trnpatterns):
        print "do runnet now"
        #forecast=123.456
        #return forecast
        #return self.test(trnpatterns)
        return self.update(trnpatterns[0][0])


"""
def demo():
    # Teach network XOR function
    pat = [
        [[0,0], [0]],
        [[0,1], [1]],
        [[1,0], [1]],
        [[1,1], [0]]
    ]

    # create a network with two input, two hidden, and one output nodes
    n = NN(2, 2, 1)
    # train it with some patterns
    n.train(pat)
    # test it
    n.test(pat)



if __name__ == '__main__':
    demo()
"""

