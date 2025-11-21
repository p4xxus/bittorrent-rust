This project started out on codecrafters: [link to the challenge](https://app.codecrafters.io/courses/bittorrent/introduction)

# a BitTorrent implementation in Rust

I somehow found pretty big interest in peer-to-peer communication. I found a challenge where you implement BitTorrent by yourself from scratch on a pretty cool coding website (see above). Now I completed the main part (it's only partial on their website) and want to implement the rest of the [protocol](https://bittorrent.org/beps/bep_0003.html).

## current actual status:
You can download basically most of the torrents which are only single file.
This means you can go to some site like piratebay (which is quite nice for testing since there aren't so many magnet link providers with active peers out there), copy the magnet like and put it in the program like so: `cargo r --release -- download_magnet [YOUR_MAGNET_LINK]`.
It saves the current state to disk so if you want to cancel the program and start it again, it will continue where it left of.
You can also seed but I don't really announce to the tracker information about files we have but not downloading. Also reading from disk is not optimised at all.

## features (done / want to add)
- [X] downloading a file from the peers of the tracker found in the .torrent file
- [X] added the ut_metadata extension to support magnet_links
- [x] seeding a file (I sadly cannot really test it right now though)
- [X] storing the state of a file to disk so that you can stop a download and continue later
- [x] periodically requesting the tracker to get peers and shit
- [x] pausing and resuming torrents
- [ ] choking algorithm

### BEP
- [Original protocol](https://bittorrent.org/beps/bep_0003.html): as I said, no choking, no optimised file reading and only partially tracker announces
- [Extension Protocol](https://www.bittorrent.org/beps/bep_0010.html)
- [Extension for Peers to Send Metadata Files](https://www.bittorrent.org/beps/bep_0009.html)
- [Tracker Returns Compact Peer Lists](https://www.bittorrent.org/beps/bep_0023.html)
- [IPv6 Tracker Extension](https://www.bittorrent.org/beps/bep_0007.html)

## current usage
Install Rust.
Run via ```cargo r --release```(same as ```cargo r --realease -p tui-ui```).
This will launch a terminal-user-interface which displays all the torrents.

**Description of the ui:**
- on the right there are logs
- on the left you can see the list of torrents you have/download (empty on initial run)
- add a new torrent by pressing `+` and paste the magnet-link or the path to the .torrent file
- go through the torrents by pressing `j` or `DOWN ARROW` / `k` or `UP ARROW` and click `SPACE` to pause/resume a torrent
- click `q` or `ESC` to quit

Note that there's no help page inside it *yet*, I will likely add one.
