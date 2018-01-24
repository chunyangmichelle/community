import sys

from pyspark import SparkContext

sc = SparkContext()
filename=sys.argv[1]
#filename="C:/Users/user/Desktop/ratings.csv"
ratings = sc.textFile(filename)
headers = ratings.top(1)
rating = ratings.filter(lambda line: line != headers[0]).map(lambda line: line.split(",")).map(
    lambda x: (x[0], [x[1]])).reduceByKey(lambda a, b: a + b)
# all [(user,[movie1,movie2]),]
# print rating.count() 671
x = rating.collect()

l = []
for i in range(len(x) - 1):
    for j in range(i + 1, len(x)):
        common = list(set(x[i][1]).intersection(x[j][1]))
        if len(common) >= 3:
            l.append([int(x[i][0]), int(x[j][0])])

# print l

import networkx as nx
import community

G = nx.Graph()
G.add_edges_from(l)

def Betweenness(G):
    betweenness = dict.fromkeys(G, 0.0)
    betweenness.update(dict.fromkeys(G.edges(), 0.0))
    for s in G.nodes():
        S, P, sigma = SP(G, s) #find the shortest path from source to target
        betweenness = sum(betweenness, S, P, sigma, s) #aggregate by edges
    for n in G:
        del betweenness[n]
    for v in betweenness:
        betweenness[v]=betweenness[v]/2
    return betweenness

def SP(G, s):
    S = []
    P = {}
    for v in G:
        P[v] = []
    sigma = dict.fromkeys(G, 0.0)
    D = {}
    sigma[s] = 1.0 #sigma is the shortest path
    D[s] = 0
    Q = [s]
    while Q:   # use BFS to find shortest paths
        v = Q.pop(0)
        S.append(v)
        Dv = D[v]
        sigmav = sigma[v]
        for w in G[v]:
            if w not in D:
                Q.append(w)
                D[w] = Dv + 1
            if D[w] == Dv + 1:
                sigma[w] += sigmav
                P[w].append(v)
    return S, P, sigma

def sum(betweenness, S, P, sigma, s):
    delta = dict.fromkeys(S, 0)
    while S:
        w = S.pop()
        for v in P[w]:
            c = sigma[v] * (1.0 + delta[w]) / sigma[w]
            if (v, w) not in betweenness:
                betweenness[(w, v)] = betweenness[(w, v)] + c
            else:
                betweenness[(v, w)] = betweenness[(v, w)] + c
            delta[v] = delta[v] + c
        if w != s:
            betweenness[w] = betweenness[w] + delta[w]
    return betweenness

# Copyright (C) 2004-2016 by Aric Hagberg <hagberg@lanl.gov> Dan Schult <dschult@colgate.edu> Pieter Swart <swart@lanl.gov> All rights reserved. SD license. sources:networkx library
#betweenness=nx.edge_betweenness_centrality(G,normalized=False)

betweenness=Betweenness(G)
y=sorted(betweenness.items())

namebet=sys.argv[3]
outputbet = open(namebet, "w")
for i in y:
    outputbet.write("("+str(i[0][0])+","+str(i[0][1])+","+str(i[1])+")" + "\n")
    #print "("+str(i[0][0])+","+str(i[0][1])+","+str(i[1])+")"

def maxbet(betweenness):
    maxedge = []
    maxv = max(betweenness.values())
    for (k, v) in betweenness.items():
        if v == maxv:
            maxedge.append(k)
    return maxedge

def Community(G):
    modularity = 0.0
    partition = {}
    betweenness = edge_betweenness_centrality(G)
    while 1:
        maxedge = maxbet(betweenness)
        for i in maxedge:
            del betweenness[i]
        if len(G.edges()) == len(maxedge):
            break
        G.remove_edges_from(maxedge)
        components = nx.connected_components(G)
        i = 0
        temp = {}
        for component in components:
            for com in list(component):
                temp[com]= i
            i = i+ 1
        tempmod = community.modularity(temp, G)
        print tempmod
        if tempmod < modularity:
            break
        else:
            partition = temp

        modularity = tempmod
    return partition
output = Community(G) 
#output = community.best_partition(G)
def reverse(d):
    dinv = {}
    for k, v in d.items():
        if v in dinv:
            dinv[v].append(k)
        else:
            dinv[v] = [k]
    return dinv


namecomm=sys.argv[2]
outputcomm = open(namecomm, "w")
for x in reverse(output).values():
    outputcomm.write (str(x)+"\n")
