use super::{internal::BPlusTreeInternalPage, leaf::BPlusTreeLeafPage};
use anyhow::Result;
use getset::{Getters, Setters};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BPlusTreeNode<KeyType, ValueType> {
    Internal(BPlusTreeInternalPage<KeyType, ValueType>),
    Leaf(BPlusTreeLeafPage<KeyType, ValueType>),
}

impl<KeyType, ValueType> BPlusTreeNode<KeyType, ValueType> {
    // TODO: common operations that can be applied to both node types.
}

#[derive(Debug, Clone, Getters, Setters, TypedBuilder, Serialize, Deserialize)]
#[getset(get = "pub", set = "pub")]
pub struct BPlusTree<KeyType, ValueType> {
    root: Option<BPlusTreeNode<KeyType, ValueType>>,
    height: usize,
    // Other necessary fields like page size, etc.
}

impl<KeyType, ValueType> BPlusTree<KeyType, ValueType>
where
    KeyType: Debug + Ord + Clone,
    ValueType: Debug + Clone,
{
    /// Creates a new B+ Tree.
    pub fn new() -> Self {
        BPlusTree::builder().root(None).height(0).build()
    }

    /// Searches for a value by a key.
    pub fn search(&self, key: &KeyType) -> Result<Option<ValueType>> {
        match &self.root {
            Some(node) => self.search_node(node, key),
            // Some(node) => todo!(),
            None => Ok(None),
        }
    }

    /// Searches for a value by a key in a specific node.
    fn search_node(
        &self,
        node: &BPlusTreeNode<KeyType, ValueType>,
        key: &KeyType,
    ) -> Result<Option<ValueType>> {
        println!("Searching in node: {:#?}", node);
        match node {
            BPlusTreeNode::Internal(internal_node) => {
                let child_index = internal_node.find_child_index(key)?;
                // Assuming `load_child` is a method to load a child node from a given index.
                let child_node = self.load_child(internal_node, child_index)?;
                self.search_node(&child_node, key)
            }
            BPlusTreeNode::Leaf(leaf_node) => Ok(leaf_node.find_value(key).ok()),
        }
    }

    /// Inserts a key-value pair into the tree, starting at a specific node.
    fn insert_node(
        &mut self,
        node: &mut BPlusTreeNode<KeyType, ValueType>,
        key: KeyType,
        value: ValueType,
    ) -> Result<Option<BPlusTreeNode<KeyType, ValueType>>> {
        println!("Inserting {:?} -> {:?} in node: {:#?}", key, value, node);
        match node {
            BPlusTreeNode::Internal(internal_node) => {
                let child_index = internal_node.find_child_index(&key)?;
                let mut child_node = self.load_child(internal_node, child_index)?;

                if let Some(new_sibling) = self.insert_node(&mut child_node, key, value)? {
                    // Handle node splitting...
                }
                Ok(None)
            }
            BPlusTreeNode::Leaf(leaf_node) => {
                leaf_node.insert(key, value)?;
                if leaf_node.is_full() {
                    Ok(Some(BPlusTreeNode::Leaf(leaf_node.split())))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub fn insert(&mut self, key: KeyType, value: ValueType) -> Result<()> {
        let mut root = self.root.take(); // Temporarily take ownership

        match root.as_mut() {
            Some(node) => {
                if let Some(new_sibling) = self.insert_node(node, key, value)? {
                    // Handle the case where the root splits
                }
            }
            None => {
                // Initialize root for empty tree
                root = Some(BPlusTreeNode::Leaf(
                    BPlusTreeLeafPage::new_with_first_entry(key, value, 10), // TODO: page size
                ));
            }
        }
        self.root = root; // Put the root back
        Ok(())
    }

    pub fn delete(&mut self, key: &KeyType) -> Result<()> {
        let mut root = self.root.take(); // Temporarily take ownership

        if let Some(node) = root.as_mut() {
            self.delete_node(node, key)?;
            // Handle potential underflow at the root
        }

        self.root = root; // Put the root back
        Ok(())
    }

    /// Deletes a key-value pair from the tree, starting at a specific node.
    fn delete_node(
        &mut self,
        node: &mut BPlusTreeNode<KeyType, ValueType>,
        key: &KeyType,
    ) -> Result<()> {
        match node {
            BPlusTreeNode::Internal(internal_node) => {
                let child_index = internal_node.find_child_index(key)?;
                let mut child_node = self.load_child(internal_node, child_index)?;

                self.delete_node(&mut child_node, key)?;
                // Handle node merging if needed...
                Ok(())
            }
            BPlusTreeNode::Leaf(leaf_node) => Ok(leaf_node.delete(key)?),
        }
    }

    /// Returns whether the tree is empty.
    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    fn load_child(
        &self,
        internal_node: &BPlusTreeInternalPage<KeyType, ValueType>,
        index: usize,
    ) -> Result<BPlusTreeNode<KeyType, ValueType>> {
        // logic to load a child node
        todo!()
    }

    // Additional helper methods for tree manipulation...
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_tree() {
        let tree: BPlusTree<i32, String> = BPlusTree::new();
        assert!(tree.is_empty());
    }

    #[test]
    fn test_insert_and_search() {
        let mut tree = BPlusTree::new();
        tree.insert(1, "Value 1".to_string()).unwrap();
        assert_eq!(
            tree.search(&1).expect("Failed to search"),
            Some("Value 1".to_string())
        );
    }

    #[test]
    fn test_delete() {
        let mut tree = BPlusTree::new();
        tree.insert(1, "Value 1".to_string()).unwrap();
        tree.delete(&1).unwrap();
        assert!(tree.search(&1).unwrap().is_none());
    }

    #[test]
    #[ignore = "Not implemented yet"]
    fn test_insert_multiple_and_search() {
        let mut tree = BPlusTree::new();
        for i in 0..100 {
            tree.insert(i, format!("Value {}", i)).unwrap();
        }

        for i in 0..100 {
            assert_eq!(
                tree.search(&i).expect("Failed to search"),
                Some(format!("Value {}", i))
            );
        }
    }

    #[test]
    #[ignore = "Not implemented yet"]
    fn test_node_splitting() {
        let mut tree: BPlusTree<i32, String> = BPlusTree::new();
        // Insert enough items to trigger a split
        // ...

        // Assertions to check the structure of the tree after the split
        // ...
    }

    // More tests covering edge cases, error handling, and complex scenarios
}
