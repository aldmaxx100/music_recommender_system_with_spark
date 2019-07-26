Hello,

Created this standard Music recommender system using Random walk algorithm.


Algorithm details:https://neo4j.com/docs/graph-algorithms/current/algorithms/random-walk/

The program uses spark dataframes to process and select top 40 recommended tracks for the user.

Dataset description used is as follow:-

userdata:<userid><trackid><artistid><timestamp>
metadata:<id><Name><artist>

The program works on principle of taste graph.

Thus using dataset userdata we need to create various edges in taste graph with their balancing weight ie

1.track1 is similar to track2 if user has listened it with 7 min timeframe.  having balancing weight 1
2.user-track edge is amount of time user has listened to track having balancing weight 0.5
3.user-artist edges is amount of time user has listened to songs from this artist having balancing weight 0.5
4.artist-track edge is relation between artist and track. having balancing weight 1

Balancing weight is required to give priority or specify that this edge has strong connection.


Now to begin random walk, we require:
if we have graph represented in format: [Vertex1 vertex1_type edge_weight vertex2 Vertex2_type ] 
1 User 0.4 2 track
1 User 0.2 3 artist
1 User 0.4 4 track
2 track 0.5 5 track





We have total 5 vertex in our graph,Thus our vector will be of length 5=[1,2,3,4,5]
vector(u)=initialize source vertex ie from where we need to start ..In this case we want to recommended songs for 
user 1 thus
vector(u)=[1,0,0,0,0]
vector(x)=Initialize weight for all vertex=[0.2,0.2,0.2,0.2,02]=initial vector ..this will be ouru final vector as well.

now we can do as many iteration as we want till we get a stationary vector(x)


thus  vector(x)=alpha*vector(u)+(1-alpha)(summation of (for each vertex in ini_vector,calculate next(v)*value of weight in vector))


where next(v)=(weight of edges outgoing from v) * balancing_weight
alpha is probablity that person will be bored 
eg; In our graph for iteration one:
lets calculate next of vertex 1
next(1)=(edge weight from user to track 2)* balancing_weight ,(edge weight from user to artist 3)* balancing_weight,(edge weight from user to track 4)* balancing_weight
		=0.4*0.5,0.2*0.5,0.4*0.5
		=[0,0.2,0.1,0.2,0]
and so on for vertex 2 ,3,4,5

thus we will get:
vector(x)=0.15[1,0,0,0,0]+0.85*0.2*next(1)+0.85*0.2*next(2)+0.85*0.2*next(3)+0.85*0.2*next(4)+0.85*0.2*next(5)

add all relevant edges in above formula to get vector (x)

repeat process till vector x is stationary.


Now in stationary vector x, if we sort element by their weight,then high weight items are likely to be recommended to user.
