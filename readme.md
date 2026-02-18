
## Design decisions explained
### Acknowledgements for requests posted to DB
+ we can set an upper bound for how long the first phase of 2PC might take, as we can control the clients response time using timeouts
+ our problem is the fact that the server might crash during processing the request in the first stage, in this case it might even have already applied the transaction because all the participants have already accepted the transaction
    + if the client was to retry the same request again this might potentially lead to duplication of transaction => requests need to be made idempotent
    + there needs to be a way for the client to know what happened to their transaction if the initial request timed out
    + to simplify for this project we will use an asynchronous with transaction ids that can be used to request the status of the transaction with that id
    + potentially this system could be extended in the future so that transaction which status are determined fast can get synchronous responses while slow ones can be requested later asynchronously
