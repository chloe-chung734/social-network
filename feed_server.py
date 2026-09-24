class FeedServer:
    def __init__(self):
        """
        This initializes the feed server.
        * Initialize any data structures needed for the feeds
        * Initialize connection with other servers or database
        """
        self.posts = []

    def add_post(self, post_id, user_id, username, caption):
        """
        add a post to a user's feed
        * would Retrieve post information from the post server
        * would determine which user should recieve the post
        * add the post the correct feeds
        """
        post = {"post_id": post_id,
                "user_id": user_id,
                "username": username,
                "caption": caption
            }

        self.posts.append(post)

    def get_feed(self, user_id):
        """
        get the feed for a specific user
        * would get users's following list from profile server
        * would get post from those users from the post server
        * would sort the post in chronological order
        * returns the completed feed
        """
        print(f"Getting feed for user {user_id}")
        return self.posts

    def display_feed(self, user_id):
        """
        displays the user's feed
        * call get_feed() to retrieve the users's feed
        * display each post
        """
        feed = self.get_feed(user_id)
        print("User Feed\n")
        for post in feed:
            print(f"Username: {post['username']}")
            print(f"caption: {post['caption']}")


if __name__ == "__main__":
    feed_server = FeedServer()
    # testing 
    feed_server.add_post(post_id = 1, user_id = 1, username = "andrew", caption = "Feed successfully posted")

    feed_server.display_feed(user_id = 1)
