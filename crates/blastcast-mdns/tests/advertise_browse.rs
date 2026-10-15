//! Advertise on one responder and find it from another over the host's real
//! interfaces, as a player on the LAN would.

use std::time::Duration;

use blastcast_mdns::{BrowseEvent, Mdns, MdnsConfig, Service, TxtEntry};

const TEST_TYPE: &str = "_tas-test._tcp.local.";
const WAIT: Duration = Duration::from_secs(10);

/// An instance name no other test run on this network is using.
fn unique(label: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("{label}-{}-{nanos}", std::process::id())
}

async fn next_resolved(
    browser: &mut blastcast_mdns::Browser,
    fullname: &str,
) -> blastcast_mdns::Discovered {
    tokio::time::timeout(WAIT, async {
        loop {
            match browser.next().await.expect("browse ended") {
                BrowseEvent::Resolved(d) if d.fullname == fullname => return d,
                _ => {}
            }
        }
    })
    .await
    .expect("advert was not resolved in time")
}

#[tokio::test]
async fn advert_is_browsed_updated_and_removed() {
    let instance = unique("player");
    let host = unique("tas-test-host");
    let server = Mdns::new(&host, &MdnsConfig::default()).unwrap();
    let client = Mdns::new(&unique("tas-test-browser"), &MdnsConfig::default()).unwrap();

    let service = Service {
        service_type: TEST_TYPE.to_owned(),
        instance: instance.clone(),
        port: 18928,
        txt: vec![
            TxtEntry::new("path", "/sendspin"),
            TxtEntry::new("name", "Kitchen"),
            TxtEntry::flag("beta"),
        ],
    };
    let fullname = service.fullname();
    let mut browser = client.browse(TEST_TYPE).unwrap();
    let mut advert = server.advertise(service.clone()).unwrap();

    let found = next_resolved(&mut browser, &fullname).await;
    assert_eq!(found.instance, instance);
    assert_eq!(found.port, 18928);
    assert_eq!(found.host, format!("{host}.local."));
    assert_eq!(found.txt_value("name"), Some("Kitchen"));
    assert_eq!(found.txt.get("beta"), Some(&None));
    assert!(!found.addresses.is_empty());
    let player = found.sendspin_client().expect("dialable");
    assert_eq!(player.name, "Kitchen");
    assert_eq!(player.path, "/sendspin");

    advert
        .update(Service {
            txt: vec![TxtEntry::new("name", "Kitchen 2")],
            ..service
        })
        .unwrap();
    tokio::time::timeout(WAIT, async {
        loop {
            if let BrowseEvent::Resolved(d) = browser.next().await.unwrap()
                && d.fullname == fullname
                && d.txt_value("name") == Some("Kitchen 2")
            {
                return;
            }
        }
    })
    .await
    .expect("update was not seen in time");

    drop(advert);
    tokio::time::timeout(WAIT, async {
        loop {
            if let BrowseEvent::Removed { fullname: gone } = browser.next().await.unwrap()
                && gone == fullname
            {
                return;
            }
        }
    })
    .await
    .expect("goodbye was not seen in time");
}
