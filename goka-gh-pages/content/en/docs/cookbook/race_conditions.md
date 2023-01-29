Distributed race conditions

describe the standard scenario, when Processor A and B consume the same input, but A has a view on B's state.
Then A won't see the immediate results of the incoming message. 
The only solution would be to introduce a new message that forwards the state.