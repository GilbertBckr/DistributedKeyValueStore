
## Design decisions explained
### Acknowledgements for requests posted to DB
+ we can set an upper bound for how long the first phase of 2PC might take, as we can control the clients response time using timeouts
+ our problem is the fact that the server might crash during processing the request in the first stage, in this case it might even have already applied the transaction because all the participants have already accepted the transaction
    + if the client was to retry the same request again this might potentially lead to duplication of transaction => requests need to be made idempotent
    + there needs to be a way for the client to know what happened to their transaction if the initial request timed out
    + to simplify for this project we will use an asynchronous with transaction ids that can be used to request the status of the transaction with that id
    + potentially this system could be extended in the future so that transaction which status are determined fast can get synchronous responses while slow ones can be requested later asynchronously

## Edge cases to consider
+ with the synchronous response model, how will requests be retried that failed during normal execution due to errors with the database 
    + imagine the following scenario a transaction is started and all the votes can are collected but when they are supposed to be written back to the database it might be locked, when we do not have an async retry mechanism in the background then the transaction will not be processed further
    + but if we do have go routine doing retries we need to ensure that there is no race condition with the retrier retrying the transaction while the request handler is still trying to gather the votes 

